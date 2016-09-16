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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kurento.client.internal.NotEnoughResourcesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 * Handler (application and media logic).
 *
 * @author Boni Garcia (boni.garcia@urjc.es)
 * @since 6.5.1
 */
public class NetworkBenchmarkHandler extends TextWebSocketHandler {

  private final Logger log = LoggerFactory.getLogger(NetworkBenchmarkHandler.class);

  private Map<String, Map<String, UserSession>> viewers = new ConcurrentHashMap<>();
  private Map<String, UserSession> presenters = new ConcurrentHashMap<>();

  @Override
  public void handleTextMessage(WebSocketSession wsSession, TextMessage message) throws Exception {
    String sessionNumber = null;
    try {
      JsonObject jsonMessage =
          new GsonBuilder().create().fromJson(message.getPayload(), JsonObject.class);

      sessionNumber = jsonMessage.get("sessionNumber").getAsString();
      log.debug("[Session number {} - WS session {}] Incoming message {}", sessionNumber,
          wsSession.getId(), jsonMessage);

      switch (jsonMessage.get("id").getAsString()) {
        case "presenter":
          presenter(wsSession, sessionNumber, jsonMessage);
          break;
        case "viewer":
          viewer(wsSession, sessionNumber, jsonMessage);
          break;
        case "onIceCandidate":
          onIceCandidate(wsSession, sessionNumber, jsonMessage);
          break;
        case "stop":
          stop(wsSession, sessionNumber);
        default:
          break;
      }

    } catch (NotEnoughResourcesException e) {
      log.warn("[Session number {} - WS session {}] Not enough resources", sessionNumber,
          wsSession.getId(), e);
      notEnoughResources(wsSession, sessionNumber);

    } catch (Throwable t) {
      log.error("[Session number {} - WS session {}] Exception in handler", sessionNumber,
          wsSession.getId(), t);
      handleErrorResponse(wsSession, sessionNumber, t);
    }
  }

  private synchronized void presenter(WebSocketSession wsSession, String sessionNumber,
      JsonObject jsonMessage) {
    if (presenters.containsKey(sessionNumber)) {
      JsonObject response = new JsonObject();
      response.addProperty("id", "presenterResponse");
      response.addProperty("response", "rejected");
      response.addProperty("message", "Another user is currently acting as sender for session "
          + sessionNumber + ". Chose another session number ot try again later ...");
      sendMessage(wsSession, sessionNumber, new TextMessage(response.toString()));

    } else {
      int bandwidth = jsonMessage.getAsJsonPrimitive("bandwidth").getAsInt();
      UserSession presenterSession = new UserSession(wsSession, sessionNumber, this, bandwidth);
      presenters.put(sessionNumber, presenterSession);

      String sdpOffer = jsonMessage.getAsJsonPrimitive("sdpOffer").getAsString();
      int loadPoints = jsonMessage.getAsJsonPrimitive("loadPoints").getAsInt();
      presenterSession.initPresenter(sdpOffer, loadPoints);
    }
  }

  private synchronized void viewer(WebSocketSession wsSession, String sessionNumber,
      JsonObject jsonMessage) {
    String wsSessionId = wsSession.getId();

    if (presenters.containsKey(sessionNumber)) {
      // Entry for viewers map
      Map<String, UserSession> viewersPerPresenter;
      if (viewers.containsKey(sessionNumber)) {
        viewersPerPresenter = viewers.get(sessionNumber);
      } else {
        viewersPerPresenter = new ConcurrentHashMap<>();
        viewers.put(sessionNumber, viewersPerPresenter);
      }

      if (viewersPerPresenter.containsKey(wsSessionId)) {
        JsonObject response = new JsonObject();
        response.addProperty("id", "viewerResponse");
        response.addProperty("response", "rejected");
        response.addProperty("message", "You are already viewing in session number "
            + viewersPerPresenter + ". Use a different browser/tab to add additional viewers.");
        sendMessage(wsSession, sessionNumber, new TextMessage(response.toString()));
      } else {
        int bandwidth = jsonMessage.getAsJsonPrimitive("bandwidth").getAsInt();
        UserSession viewerSession = new UserSession(wsSession, sessionNumber, this, bandwidth);
        viewersPerPresenter.put(wsSessionId, viewerSession);

        viewerSession.initViewer(presenters.get(sessionNumber), jsonMessage);
      }

    } else {
      JsonObject response = new JsonObject();
      response.addProperty("id", "viewerResponse");
      response.addProperty("response", "rejected");
      response.addProperty("message", "No active presenter for sesssion number " + sessionNumber
          + " now. Become sender or try again later ...");
      sendMessage(wsSession, sessionNumber, new TextMessage(response.toString()));
    }
  }

  private synchronized void stop(WebSocketSession wsSession, String sessionNumber) {
    String wsSessionId = wsSession.getId();
    Map<String, UserSession> viewersPerPresenter = null;
    UserSession userSession = findUserByWsSession(wsSession);
    log.info("[Session number {} - WS session {}] Stopping session", sessionNumber, wsSessionId);

    if (presenters.containsKey(sessionNumber) && presenters.get(sessionNumber).getWebSocketSession()
        .getId().equals(userSession.getWebSocketSession().getId())) {
      // Case 1. Stop arrive from presenter
      log.info("[Session number {} - WS session {}] Releasing presenter", sessionNumber,
          wsSessionId);

      if (viewers.containsKey(sessionNumber)) {
        viewersPerPresenter = viewers.get(sessionNumber);
        logViewers(sessionNumber, wsSessionId, viewersPerPresenter);

        for (UserSession viewer : viewersPerPresenter.values()) {
          log.info(
              "[Session number {} - WS session {}] Sending stopCommunication message to viewer",
              sessionNumber, viewer.getWebSocketSession().getId());

          // Send stopCommunication to viewer
          JsonObject response = new JsonObject();
          response.addProperty("id", "stopCommunication");
          sendMessage(viewer.getWebSocketSession(), sessionNumber,
              new TextMessage(response.toString()));

          // Send latencies
          List<Double> mediaPipelineLatencies = viewersPerPresenter
              .get(viewer.getWebSocketSession().getId()).getMediaPipelineLatencies();
          List<Double> filterLatencies =
              viewersPerPresenter.get(viewer.getWebSocketSession().getId()).getFilterLatencies();
          sendStopResponse(viewer.getWebSocketSession(), viewer.getSessionNumber(),
              mediaPipelineLatencies, filterLatencies);

          // Release viewer
          viewersPerPresenter.get(viewer.getWebSocketSession().getId()).releaseViewer();
          viewersPerPresenter.remove(viewer.getWebSocketSession().getId());
        }
        // Remove viewer session from map
        viewers.remove(sessionNumber);
      }

      // Release presenter session
      presenters.get(sessionNumber).releasePresenter();

      // Remove presenter session from map
      presenters.remove(sessionNumber);

    } else if (viewers.containsKey(sessionNumber)
        && viewers.get(sessionNumber).containsKey(wsSessionId)) {
      // Case 2. Stop arrive from viewer

      // Send latencies
      List<Double> mediaPipelineLatencies =
          viewers.get(sessionNumber).get(wsSessionId).getMediaPipelineLatencies();
      List<Double> filterLatencies =
          viewers.get(sessionNumber).get(wsSessionId).getMediaPipelineLatencies();
      sendStopResponse(wsSession, sessionNumber, mediaPipelineLatencies, filterLatencies);

      viewersPerPresenter = viewers.get(sessionNumber);
      logViewers(sessionNumber, wsSessionId, viewersPerPresenter);
      viewersPerPresenter.get(wsSessionId).releaseViewer();
      viewersPerPresenter.remove(wsSessionId);
    }

    logViewers(sessionNumber, wsSessionId, viewersPerPresenter);
  }

  private void sendStopResponse(WebSocketSession wsSession, String sessionNumber,
      List<Double> mediaPipelineLatencies, List<Double> filterLatencies) {
    log.info("[Session number {} - WS session {}] Sending stopResponse message to viewer",
        sessionNumber, wsSession.getId());

    JsonObject stopResponse = new JsonObject();
    stopResponse.addProperty("id", "stopResponse");
    stopResponse.addProperty("mediaPipelineLatencies", new Gson().toJson(mediaPipelineLatencies));
    stopResponse.addProperty("filterLatencies", new Gson().toJson(filterLatencies));
    sendMessage(wsSession, sessionNumber, new TextMessage(stopResponse.toString()));
  }

  private void logViewers(String sessionNumber, String wsSessionId,
      Map<String, UserSession> viewers) {
    if (viewers != null) {
      log.info("[Session number {} - WS session {}] There are {} real viewer(s) at this moment",
          sessionNumber, wsSessionId, viewers.size());
    }
  }

  private void onIceCandidate(WebSocketSession wsSession, String sessionNumber,
      JsonObject jsonMessage) {
    JsonObject candidate = jsonMessage.get("candidate").getAsJsonObject();
    String wsSessionId = wsSession.getId();
    UserSession userSession = findUserByWsSession(wsSession);
    if (userSession != null) {
      userSession.addCandidate(candidate);
    } else {
      log.warn("[Session number {} - WS session {}] ICE candidate not valid: {}", sessionNumber,
          wsSessionId, candidate);
    }
  }

  private void handleErrorResponse(WebSocketSession wsSession, String sessionNumber,
      Throwable throwable) {
    // Send error message to client
    JsonObject response = new JsonObject();
    response.addProperty("id", "error");
    response.addProperty("response", "rejected");
    response.addProperty("message", throwable.getMessage());
    sendMessage(wsSession, sessionNumber, new TextMessage(response.toString()));
    log.error("[Session number {} - WS session {}] Error handling message", sessionNumber,
        wsSession.getId(), throwable);

    // Release media session
    stop(wsSession, sessionNumber);
  }

  private void notEnoughResources(WebSocketSession wsSession, String sessionNumber) {
    // Send notEnoughResources message to client
    JsonObject response = new JsonObject();
    response.addProperty("id", "notEnoughResources");
    sendMessage(wsSession, sessionNumber, new TextMessage(response.toString()));

    // Release media session
    stop(wsSession, sessionNumber);
  }

  private UserSession findUserByWsSession(WebSocketSession wsSession) {
    String wsSessionId = wsSession.getId();
    // Find WS session in presenters
    for (String sessionNumber : presenters.keySet()) {
      if (presenters.get(sessionNumber).getWebSocketSession().getId().equals(wsSessionId)) {
        return presenters.get(sessionNumber);
      }
    }

    // Find WS session in viewers
    for (String sessionNumber : viewers.keySet()) {
      for (UserSession userSession : viewers.get(sessionNumber).values()) {
        if (userSession.getWebSocketSession().getId().equals(wsSessionId)) {
          return userSession;
        }
      }
    }
    return null;
  }

  public synchronized void sendMessage(WebSocketSession session, String sessionNumber,
      TextMessage message) {
    try {
      log.debug("[Session number {} - WS session {}] Sending message {} in session {}",
          sessionNumber, session.getId(), message.getPayload());
      session.sendMessage(message);

    } catch (IOException e) {
      log.error("[Session number {} - WS session {}] Exception sending message", sessionNumber,
          session.getId(), e);
    }
  }

  @Override
  public void afterConnectionClosed(WebSocketSession wsSession, CloseStatus status)
      throws Exception {
    String wsSessionId = wsSession.getId();
    String sessionNumber = "-";
    UserSession userSession = findUserByWsSession(wsSession);

    if (userSession != null) {
      sessionNumber = userSession.getSessionNumber();
      stop(wsSession, sessionNumber);
    }
    log.info("[Session number {} - WS session {}] WS connection closed", sessionNumber,
        wsSessionId);
  }

}
