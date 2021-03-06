/*
 * (C) Copyright 2016 NUBOMEDIA (http://www.nubomedia.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package eu.nubomedia.network.benchmark;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kurento.client.internal.NotEnoughResourcesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

/**
 * Handler (application and media logic).
 *
 * @author Boni Garcia (boni.garcia@urjc.es)
 * @since 6.6.0
 */
public class NetworkBenchmarkHandler extends TextWebSocketHandler {

	private final Logger log = LoggerFactory.getLogger(NetworkBenchmarkHandler.class);

	private Map<String, UserSession> sessions = new ConcurrentHashMap<>();

	@Override
	public void handleTextMessage(WebSocketSession wsSession, TextMessage message) throws Exception {
		try {
			JsonObject jsonMessage = new GsonBuilder().create().fromJson(message.getPayload(), JsonObject.class);

			log.debug("[WS session {}] Incoming message {}", wsSession.getId(), jsonMessage);

			switch (jsonMessage.get("id").getAsString()) {
			case "start":
				start(wsSession, jsonMessage);
				break;
			case "onIceCandidate":
				onIceCandidate(wsSession, jsonMessage);
				break;
			case "stop":
				stop(wsSession);
			default:
				break;
			}

		} catch (NotEnoughResourcesException e) {
			log.warn("[WS session {}] Not enough resources", wsSession.getId(), e);
			notEnoughResources(wsSession);

		} catch (Throwable t) {
			log.error("[WS session {}] Exception in handler", wsSession.getId(), t);
			handleErrorResponse(wsSession, t);
		}
	}

	private synchronized void start(WebSocketSession wsSession, JsonObject jsonMessage) {
		String wsSessionId = wsSession.getId();
		if (sessions.containsKey(wsSessionId)) {
			JsonObject response = new JsonObject();
			response.addProperty("id", "startResponse");
			response.addProperty("response", "rejected");
			response.addProperty("message", "Web socket session already active");
			sendMessage(wsSession, new TextMessage(response.toString()));
		} else {
			UserSession userSession = new UserSession(wsSession, this, jsonMessage);
			sessions.put(wsSessionId, userSession);
			userSession.initSession();

			log.info("[WS session {}] Starting session {}", wsSession.getId(), sessions);
		}
	}

	private synchronized void stop(WebSocketSession wsSession) throws IOException, InterruptedException {

		String wsSessionId = wsSession.getId();
		log.info("[WS session {}] Stopping session", wsSessionId);

		UserSession userSession = sessions.get(wsSessionId);
		if (userSession != null) {
			// Release session
			userSession.releaseSession();

			// Send stopCommunication
			JsonObject response = new JsonObject();
			response.addProperty("id", "stopCommunication");

			String latencies = userSession.getLatenciesAsCsv();
			if (latencies != null && !latencies.isEmpty()) {
				response.addProperty("latencies", latencies);
			}

			sendMessage(wsSession, new TextMessage(response.toString()));

			// Remove session from list
			sessions.remove(wsSessionId);
		}
	}

	private void onIceCandidate(WebSocketSession wsSession, JsonObject jsonMessage) {
		JsonObject candidate = jsonMessage.get("candidate").getAsJsonObject();
		String wsSessionId = wsSession.getId();
		UserSession userSession = sessions.get(wsSessionId);
		if (userSession != null) {
			userSession.addCandidate(candidate);
		} else {
			log.warn("[WS session {}] ICE candidate not valid: {}", wsSessionId, candidate);
		}
	}

	private void handleErrorResponse(WebSocketSession wsSession, Throwable throwable)
			throws IOException, InterruptedException {
		// Send error message to client
		JsonObject response = new JsonObject();
		response.addProperty("id", "error");
		response.addProperty("response", "rejected");
		response.addProperty("message", throwable.getMessage());
		sendMessage(wsSession, new TextMessage(response.toString()));
		log.error("[WS session {}] Error handling message", wsSession.getId(), throwable);

		// Release media session
		stop(wsSession);
	}

	private void notEnoughResources(WebSocketSession wsSession) throws IOException, InterruptedException {
		// Send notEnoughResources message to client
		JsonObject response = new JsonObject();
		response.addProperty("id", "notEnoughResources");
		sendMessage(wsSession, new TextMessage(response.toString()));

		// Release media session
		stop(wsSession);
	}

	public synchronized void sendMessage(WebSocketSession session, TextMessage message) {
		try {
			log.debug("[WS session {}] Sending message {} in session {}", session.getId(), message.getPayload());
			session.sendMessage(message);

		} catch (IOException e) {
			log.error("[WS session {}] Exception sending message", session.getId(), e);
		}
	}

	@Override
	public void afterConnectionClosed(WebSocketSession wsSession, CloseStatus status) throws Exception {
		log.info("[WS session {}] WS connection closed", wsSession.getId());
		stop(wsSession);
	}

}
