/*
 * (C) Copyright 2016 NUBOMEDIA (http://www.nubomedia.eu)
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl-2.1.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 */

var ws = new WebSocket('wss://' + location.host + '/network-benchmark');
var video;
var webRtcPeer;

window.onload = function() {
	console = new Console();
	console["debug"] = console.info;
	video = document.getElementById('video');
	disableStopButton();

	$('input[type=radio][name=stopMethod]').change(function() {
		$('#stopTime').attr('disabled', this.value == 'manual');
	});
	$('input[type=radio][name=mediaSource]').change(function() {
		$('#mediaSourceUrl').attr('disabled', this.value == 'usermedia');
	});
}

window.onbeforeunload = function() {
	ws.close();
}

ws.onmessage = function(message) {
	var parsedMessage = JSON.parse(message.data);
	console.info('Received message: ' + message.data);

	switch (parsedMessage.id) {
	case 'startResponse':
		startResponse(parsedMessage);
		break;
	case 'stopCommunication':
		stopCommunication(parsedMessage);
		break;
	case 'iceCandidate':
		webRtcPeer.addIceCandidate(parsedMessage.candidate, function(error) {
			if (error) {
				return console.error("Error adding candidate: " + error);
			}
		});
		break;
	case 'error':
		console.error("Error message from server: " + parsedMessage.message);
		dispose();
		break;
	case 'notEnoughResources':
		stop(false);
		$('#resourcesDialog').modal('show');
		break;
	default:
		console.error('Unrecognized message', parsedMessage);
	}
}

function stopCommunication() {
	// TODO force download
	dispose();
}

function startResponse(message) {
	if (message.response != 'accepted') {
		var errorMsg = message.message ? message.message : 'Unknow error';
		console.info('Call not accepted for the following reason: ' + errorMsg);
		dispose();
	} else {
		if (message.sdpAnswer) {
			webRtcPeer.processAnswer(message.sdpAnswer, function(error) {
				if (error) {
					return console.error(error);
				}
			});
		}

		var autoStop = $('input[name=stopMethod]:checked').val() == "auto";
		if (autoStop) {
			var stopTime = document.getElementById('stopTime').value;
			console.info("Auto stop in " + stopTime + " milliseconds");
			setInterval(stop, stopTime);
		}
	}
}

function start() {
	var userMedia = $('input[name=mediaSource]:checked').val() == "usermedia";

	if (userMedia) {
		console.info("Using user media to feed WebRTC");

		if (!webRtcPeer) {
			showSpinner(video, "spinner.gif");

			var options = {
				localVideo : video,
				onicecandidate : onIceCandidate
			}
			webRtcPeer = new kurentoUtils.WebRtcPeer.WebRtcPeerSendonly(
					options, function(error) {
						if (error) {
							return console.error(error);
						}
						webRtcPeer.generateOffer(onOffer);
					});

		}
	} else {
		var mediaSourceUrl = document.getElementById('mediaSourceUrl').value;
		console.info("Using HTTP source (" + mediaSourceUrl
				+ ") to feed WebRTC");
		console
				.warn("Video tag not playing media (all processing is inside NUBOMEDIA")
		showSpinner(video, "play.gif");

		sendStartMessage();
	}

	enableStopButton();
}

function onOffer(error, sdpOffer) {
	if (error) {
		return console.error('Error generating the offer');
	}
	console.info('Invoking SDP offer callback function ' + location.host);

	sendStartMessage(sdpOffer);
}

function sendStartMessage() {
	var loadPoints = document.getElementById('loadPoints').value;
	var webrtcChannels = document.getElementById('webrtcChannels').value;
	var bandwidth = document.getElementById('bandwidth').value;
	var userMedia = $('input[name=mediaSource]:checked').val() == "usermedia";
	var sdpOffer = userMedia ? arguments[0] : "";
	var mediaSourceUrl = userMedia ? "" : document
			.getElementById('mediaSourceUrl').value;

	var message = {
		id : 'start',
		loadPoints : loadPoints,
		webrtcChannels : webrtcChannels,
		bandwidth : bandwidth,
		sdpOffer : sdpOffer,
		mediaSourceUrl : mediaSourceUrl
	}
	sendMessage(message);

}

function onIceCandidate(candidate) {
	console.log("Local candidate" + JSON.stringify(candidate));

	var message = {
		id : 'onIceCandidate',
		candidate : candidate
	};
	sendMessage(message);
}

function stop() {
	var message = {
		id : 'stop'
	}
	sendMessage(message);
	dispose();
}

function dispose() {
	if (webRtcPeer) {
		webRtcPeer.dispose();
		webRtcPeer = null;
	}
	hideSpinner(video);

	disableStopButton();
}

function disableStopButton() {
	enableButton('#start', 'start()');
	disableButton('#stop');
}

function enableStopButton() {
	disableButton('#start');
	enableButton('#stop', 'stop()');
}

function disableButton(id) {
	$(id).attr('disabled', true);
	$(id).removeAttr('onclick');
}

function enableButton(id, functionName) {
	$(id).attr('disabled', false);
	$(id).attr('onclick', functionName);
}

function sendMessage(message) {
	var jsonMessage = JSON.stringify(message);
	console.log('Senging message: ' + jsonMessage);
	ws.send(jsonMessage);
}

function showSpinner() {
	var gif = arguments[arguments.length - 1];
	for (var i = 0; i < arguments.length - 1; i++) {
		arguments[i].poster = './img/transparent.png';
		arguments[i].style.background = "center transparent url('./img/" + gif
				+ "') no-repeat";
	}
}

function hideSpinner() {
	for (var i = 0; i < arguments.length; i++) {
		arguments[i].src = '';
		arguments[i].poster = './img/webrtc.png';
		arguments[i].style.background = '';
	}
}

/**
 * Lightbox utility (to display media pipeline image in a modal dialog)
 */
$(document).delegate('*[data-toggle="lightbox"]', 'click', function(event) {
	event.preventDefault();
	$(this).ekkoLightbox();
});
