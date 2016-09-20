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

package eu.nubomedia.network.benchmark;

import java.util.ArrayList;
import java.util.List;

import org.kurento.client.EventListener;
import org.kurento.client.IceCandidate;
import org.kurento.client.KurentoClient;
import org.kurento.client.MediaPipeline;
import org.kurento.client.OnIceCandidateEvent;
import org.kurento.client.Properties;
import org.kurento.client.WebRtcEndpoint;
import org.kurento.jsonrpc.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.google.gson.JsonObject;

/**
 * User session.
 * 
 * @author Boni Garcia (boni.garcia@urjc.es)
 * @since 6.6.0
 */
public class UserSession {

  private final Logger log = LoggerFactory.getLogger(UserSession.class);

  private NetworkBenchmarkHandler handler;
  private WebSocketSession wsSession;
  private WebRtcEndpoint sourceWebRtcEndpoint;
  private KurentoClient sourceKurentoClient;
  private KurentoClient targetKurentoClient;
  private MediaPipeline sourceMediaPipeline;
  private MediaPipeline targetMediaPipeline;

  private List<Double> mediaPipelineLatencies = new ArrayList<>();
  private JsonObject jsonMessage;

  public UserSession(WebSocketSession wsSession, NetworkBenchmarkHandler handler,
      JsonObject jsonMessage) {
    this.wsSession = wsSession;
    this.handler = handler;
    this.jsonMessage = jsonMessage;
  }

  public void initSession() {
    log.info("[WS session {}] Init sesssion", wsSession.getId());

    // KurentoClients
    int bandwidth = jsonMessage.getAsJsonPrimitive("bandwidth").getAsInt();
    int loadPoints = jsonMessage.getAsJsonPrimitive("loadPoints").getAsInt();
    log.info("[WS session {}] Reserving {} points to create KurentoClient", wsSession.getId(),
        loadPoints);
    Properties properties = new Properties();
    properties.add("loadPoints", loadPoints);
    sourceKurentoClient = KurentoClient.create(properties);
    targetKurentoClient = KurentoClient.create(properties);

    // Response
    JsonObject response = new JsonObject();
    response.addProperty("id", "startResponse");
    response.addProperty("response", "accepted");

    // Media pipelines
    sourceMediaPipeline = sourceKurentoClient.createMediaPipeline();
    targetMediaPipeline = targetKurentoClient.createMediaPipeline();

    sourceWebRtcEndpoint = createWebRtcEndpoint(sourceMediaPipeline, bandwidth);
    sourceWebRtcEndpoint.addOnIceCandidateListener(new EventListener<OnIceCandidateEvent>() {
      @Override
      public void onEvent(OnIceCandidateEvent event) {
        JsonObject response = new JsonObject();
        response.addProperty("id", "iceCandidate");
        response.add("candidate", JsonUtils.toJsonObject(event.getCandidate()));
        handler.sendMessage(wsSession, new TextMessage(response.toString()));
      }
    });

    String sdpOffer = jsonMessage.getAsJsonPrimitive("sdpOffer").getAsString();
    String sdpAnswer = sourceWebRtcEndpoint.processOffer(sdpOffer);
    response.addProperty("sdpAnswer", sdpAnswer);

    sourceWebRtcEndpoint.gatherCandidates();

    int webrtcChannels = jsonMessage.getAsJsonPrimitive("webrtcChannels").getAsInt();
    for (int i = 0; i < webrtcChannels; i++) {
      WebRtcEndpoint webRtcEndpoint1 = createWebRtcEndpoint(sourceMediaPipeline, bandwidth);
      sourceWebRtcEndpoint.connect(webRtcEndpoint1);
      WebRtcEndpoint webRtcEndpoint2 = createWebRtcEndpoint(targetMediaPipeline, bandwidth);
      connectWebRtcEndpoints(webRtcEndpoint1, webRtcEndpoint2);
    }

    // Send response message
    handler.sendMessage(wsSession, new TextMessage(response.toString()));
  }

  private void connectWebRtcEndpoints(final WebRtcEndpoint webRtcEndpoint1,
      final WebRtcEndpoint webRtcEndpoint2) {
    webRtcEndpoint1.addOnIceCandidateListener(new EventListener<OnIceCandidateEvent>() {
      @Override
      public void onEvent(OnIceCandidateEvent event) {
        webRtcEndpoint2.addIceCandidate(event.getCandidate());
      }
    });

    webRtcEndpoint2.addOnIceCandidateListener(new EventListener<OnIceCandidateEvent>() {
      @Override
      public void onEvent(OnIceCandidateEvent event) {
        webRtcEndpoint1.addIceCandidate(event.getCandidate());
      }
    });

    String sdpOffer = webRtcEndpoint2.generateOffer();
    String sdpAnswer = webRtcEndpoint1.processOffer(sdpOffer);
    webRtcEndpoint2.processAnswer(sdpAnswer);

    webRtcEndpoint1.gatherCandidates();
    webRtcEndpoint2.gatherCandidates();
  }

  public void addCandidate(JsonObject jsonCandidate) {
    IceCandidate candidate = new IceCandidate(jsonCandidate.get("candidate").getAsString(),
        jsonCandidate.get("sdpMid").getAsString(), jsonCandidate.get("sdpMLineIndex").getAsInt());
    sourceWebRtcEndpoint.addIceCandidate(candidate);
  }

  public List<Object> releaseSession() {
    log.info("[WS session {}] Releasing session", wsSession.getId());

    if (sourceMediaPipeline != null) {
      log.info("[WS session {}] Releasing media pipelines", wsSession.getId());
      sourceMediaPipeline.release();
      sourceMediaPipeline = null;

      targetMediaPipeline.release();
      targetMediaPipeline = null;
    }

    if (sourceKurentoClient != null) {
      log.info("[WS session {}] Destroying kurentoClients", wsSession.getId());
      sourceKurentoClient.destroy();
      sourceKurentoClient = null;

      targetKurentoClient.destroy();
      targetKurentoClient = null;
    }

    // TODO
    return null;
  }

  private WebRtcEndpoint createWebRtcEndpoint(MediaPipeline mediaPipeline, int bandwidth) {
    WebRtcEndpoint webRtcEndpoint = new WebRtcEndpoint.Builder(mediaPipeline).build();
    webRtcEndpoint.setMaxVideoSendBandwidth(bandwidth);
    webRtcEndpoint.setMinVideoSendBandwidth(bandwidth);
    webRtcEndpoint.setMaxVideoRecvBandwidth(bandwidth);
    webRtcEndpoint.setMinVideoRecvBandwidth(bandwidth);

    return webRtcEndpoint;
  }

  public WebSocketSession getWebSocketSession() {
    return wsSession;
  }

  public MediaPipeline getMediaPipeline() {
    return sourceMediaPipeline;
  }

  public WebRtcEndpoint getWebRtcEndpoint() {
    return sourceWebRtcEndpoint;
  }

  public List<Double> getMediaPipelineLatencies() {
    return mediaPipelineLatencies;
  }

}
