const AWS = require("aws-sdk");
const fs = require("fs");
const path = require("path");

class S3Client{
  constructor(){
    this.s3Client = new AWS.S3();
  }
  
  getBuckets(){
    return new Promise((res, rej) => {
      this.s3Client.listBuckets(function(err, data) { 
        if(err){
          return rej(err);
        }
        return res((data.Buckets || []).map(({Name, CreationDate}) => {
          return {name:Name, creationDate:CreationDate};
        }));
      });
    });
  }
  
  listObjects(bucket){
    return new Promise((res, rej) => {
      this.s3Client.listObjects({Bucket:bucket}, function(err, data) {
        if (err) {
          rej(err);
        } else {
          res((data.Contents || []).map(({Key, LastModified, ETag, Size}) => {
            return {key:Key, lastModified:LastModified, eTag:ETag, size:Size};
          }));
        }
      });
    })
  }
  
  putObjectByFile(filePath, bucket, key){
    let s3Params = {Bucket:bucket, Key:key};
    return new Promise((res, rej) => {
      let fileStream = fs.createReadStream(filePath);
          fileStream.on('error', function(err) {
              rej(err);
          });
          s3Params.Body = fileStream;
          this.s3Client.upload(s3Params, (er, data) => {
              if (er) {
                rej(er);
              } else {
                res(data);
              }
          });
    });
  }
  
  putObjectByStream(stream, bucket, key){
    let s3Params = {Bucket:bucket, Key:key};
    return new Promise((res, rej) => {
          stream.on('error', function(err) {
              rej(err);
          });
          s3Params.Body = stream;
          this.s3Client.upload(s3Params, (er, data) => {
              if (er) {
                rej(er);
              } else {
                res(data);
              }
          });
    });
  }
  
  getObjectFromFile(bucket, key, localFile, name){
    let s3Params = {Bucket:bucket, Key:key};
    return new Promise((res, rej) => {
      this.s3Client.getObject(s3Params, (er, data) => {
        if (er) {
          rej(er);
        } else {
          fs.writeFile(localFile, data.Body, (e) => {
            if(e){
              rej(e);
            } else {
              res({localFile, meta:{name, metaData:data.Metadata, size:data.ContentLength, lastModified:data.LastModified, mimeType:data.ContentType}})
            }
          });
        }
      });
    });
  }
  
  getObjectFromStream(bucket, key, name){
    let s3Params = {Bucket:bucket, Key:key};
    return new Promise((res, rej) => {
      this.s3Client.getObject(s3Params, (er, data) => {
        if (er) {
          rej(er);
        } else {
          res({stream:data.Body, meta:{name, metaData:data.Metadata, size:data.ContentLength, lastModified:data.LastModified, mimeType:data.ContentType}});
        }
      });
    });
  }
  
}

module.exports = S3Client;