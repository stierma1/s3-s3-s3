let bus = require("bus-bus-bus");
let S3Service = require("./s3-service");
new S3Service();
bus.on("telemetry", (p) => {
  console.log(p);
});

(async () => {
    let buckets = await bus.requestResponse("S3Service:getBuckets")
    console.log(buckets);
    bus.emit("globalShutdown");
})();