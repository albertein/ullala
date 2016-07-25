'use strict';

function init() {
  var socket = window.io();
  socket.on('image', updateImage);
}

function updateImage(data) {
  var image = document.getElementById('player');
  var time = document.getElementById('time');    
  image.src = '/images/' + data.path;
  time.innerText = getConsumerLag(data.captureTimestamp)  + ' seconds of consumer lag';
}

function getConsumerLag(captureTimestamp) {
  const captureDate = new Date(0);
  captureDate.setUTCSeconds(captureTimestamp);
  return (new Date() - captureDate) / 1000;
}

init();
