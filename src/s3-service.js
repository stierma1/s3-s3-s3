const S3Client = require("./aws-s3");
const bus = require("bus-bus-bus");

class S3Service{
  constructor(){
    this.s3Client = new S3Client();
    this.outstandingRequests = 0;
    this.mailbox = [];
    this.maxOutstanding = 6;
    this.telemetryInterval = setInterval(() => {
      bus.emit("telemetry", {emitter:"s3-s3-s3", name:"s3.telemetry", outstandingRequests:this.outstandingRequests, mailboxSize:this.mailbox.length});
    }, 5000);
    
    bus.on("globalShutdown", () => {
      clearInterval(this.telemetryInterval);
    });
    
    bus.on("S3Service:getBuckets", this.callClient("S3Service:getBuckets", async (returnService) => {
      try{
        let buckets = await this.s3Client.getBuckets();
        bus.emit(returnService, null, buckets);
      } catch(err){
        bus.emit(returnService, err);
      }
    }));
    bus.on("S3Service:listObjects", this.callClient("S3Service:listObjects", async (returnService, {bucket}) => {
      try{
        let objects = await this.s3Client.listObjects(bucket);
        bus.emit(returnService, null, objects);
      } catch(err){
        bus.emit(returnService, err);
      }
    }));
    bus.on("S3Service:putObjectByFile", this.callClient("S3Service:putObjectByFile", async (returnService, {filePath, bucket, key}) => {
      try{
        let meta = await this.s3Client.putObjectByFile(filePath, bucket, key);
        bus.emit(returnService, null, meta);
      } catch(err){
        bus.emit(returnService, err);
      }
    }));
    bus.on("S3Service:putObjectByStream", this.callClient("S3Service:putObjectByStream", async (returnService, {stream, bucket, key}) => {
      try{
        let meta = await this.s3Client.putObjectByFile(stream, bucket, key);
        bus.emit(returnService, null, meta);
      } catch(err){
        bus.emit(returnService, err);
      }
    }));
    bus.on("S3Service:getObjectFromFile", this.callClient("S3Service:getObjectFromFile", async (returnService, {bucket, key, localFile, name}) => {
      try{
        let meta = await this.s3Client.getObjectByFile(bucket, key, localFile, name);
        bus.emit(returnService, null, meta);
      } catch(err){
        bus.emit(returnService, err);
      }
    }));
    bus.on("S3Service:getObjectFromStream", this.callClient("S3Service:getObjectFromStream", async (returnService, {bucket, key, name}) => {
      try{
        let {meta, stream} = await this.s3Client.getObjectByFile(bucket, key, name);
        bus.emit(returnService, null, {meta, stream});
      } catch(err){
        bus.emit(returnService, err);
      }
    }));
  }
  
  async invoke(){
    if(this.mailbox.length > 0 && this.maxOutstanding >= this.outstandingRequests){
      let {event, returnService, params, invokeFunc} = this.mailbox.shift();
      bus.emit("log", {emitter:"s3-s3-s3", message:"Request started", event, params, returnService});
      let timer = Date.now();
      try{
        await invokeFunc();
        bus.emit("telemetry", {emitter:"s3-s3-s3", time:Date.now() - timer, event, params, status:"success", returnService, name:"s3.request.time"})
      } catch(e){
        bus.emit("telemetry", {emitter:"s3-s3-s3", time:Date.now() - timer, event, params, status:"error", returnService, name:"s3.request.time", error:e})
      }
    }
  }
  
  callClient(event, func){
    return (returnService, params) => {
      this.mailbox.push({event, returnService, params, invokeFunc: async () => {
        this.outstandingRequests++;
        try{
          await func(returnService, params);
        } catch(err){
          throw err;
        } finally{
          this.outstandingRequests--;
          setTimeout(() => {this.invoke();}, 0);
        }
      }});
      setTimeout(() => {this.invoke();}, 0);
    }
  }
}

module.exports = S3Service;