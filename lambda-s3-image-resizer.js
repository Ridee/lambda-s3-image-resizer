// dependencies
var async = require('async');
var AWS = require('aws-sdk');
var gm = require('gm').subClass({imageMagick: true}); // Enable ImageMagick integration.
var util = require('util');

// constants
var MAX_WIDTH = 900;
var MAX_HEIGHT = 900;

// get reference to S3 client
var s3 = new AWS.S3();

exports.handler = function (event, context, callback) {
  var origPrepend = 'orig-';

  // Read options from the event.
  console.log("Reading options from event:\n",
    util.inspect(event, {depth: 5}));
  var bucket = event.Records[0].s3.bucket.name;
  // Object key may have spaces or unicode non-ASCII characters.
  var curKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));

  // Various checks to ensure we haven't already processed the image
  // and we can do it.
  if (curKey.lastIndexOf(origPrepend, 0) === 0) {
    callback("Image already resized");
    return;
  }
  console.log("This assumes what you give it is an image file, as we strip file endings in uploads.")

  // Where to store the original file
  var origKey = origPrepend + curKey;
  console.log('Downloading file ' + curKey + ' and copying to ' + origKey + '.');
  console.log('');
  // Download the image from S3, transform, and upload to a different S3 bucket.
  async.waterfall(
    [
      function check_existing(next) {
        console.log('Checking existing')
        // Check file metadata for the "resized" mark.
        s3.headObject({
          Bucket: bucket,
          Key: origKey
        }, function (err, data) {
          if (err) {
            next(null);
          } else {
            next('File was already resized');
          }
        });
      },
      function download(next) {
        // Download the image from S3 into a buffer.
        console.log('Finished checking if image already resized.')
        console.log('Downloading image');
        s3.getObject({
            Bucket: bucket,
            Key: curKey
          },
          next);
      },
      function checkFormat(response, next) {
        console.log('Finished donwloading image');
        console.log('Checking image format');
        gm(response.Body).format(function(err, value){
          console.log(value);
          if(value !== undefined && value == 'JPEG'){
            next(null, response);
          } else {
            next('File has an inappropriate format');
          }
        });
      },
      function transform(response, next) {
        console.log('Finished checking image format');
        console.log('Resizing image');
        gm(response.Body).size(function (err, size) {
          // Infer the scaling factor to avoid stretching the image unnaturally.
          var scalingFactor = Math.min(
            MAX_WIDTH / size.width,
            MAX_HEIGHT / size.height
          );
          var width = scalingFactor * size.width;
          var height = scalingFactor * size.height;

          // Transform the image buffer in memory.
          this.resize(width, height)
            .toBuffer("jpg", function (err, buffer) {
              if (err) {
                next(err);
              } else {
                next(null, response.ContentType, buffer);
              }
            });
        });
      },
      function copyBackup(contentType, data, next) {
        console.log('Finished resizing image');
        console.log('Copying backup');
        // Copy the original object into the rezied name
        s3.copyObject({
          Bucket: bucket, /* required */
          CopySource: bucket + '/' + curKey, /* required */
          Key: origKey /* required */
        }, function (err, data2) {
          if (err) {
            next(err);
          } else {
            console.log('Finished copying backup');
            next(null, contentType, data);
          }
        });
      },
      function uploadFile(contentType, data, next) {
        console.log('Uploading')
        // Stream the transformed image to a different S3 bucket.
        s3.putObject({
            Bucket: bucket,
            Key: curKey,
            Body: data,
            ContentType: contentType
          },
          next);
        console.log('Finished uploading');
      }
    ],
    function (err) {
      if (err) {
        console.log(
          'Unable to resize ' + bucket + '/' + curKey +
          ' and upload to ' + bucket + '/' + origKey +
          ' due to an error: ' + err
        );
      }
      else {
        console.log(
          'Successfully resized ' + bucket + '/' + curKey +
          ' and uploaded to ' + bucket + '/' + origKey
        );
      }
    }
  );
};
