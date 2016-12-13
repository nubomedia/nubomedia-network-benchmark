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
import java.io.StringWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.kurento.client.EndpointStats;
import org.kurento.client.EventListener;
import org.kurento.client.IceCandidate;
import org.kurento.client.KurentoClient;
import org.kurento.client.MediaElement;
import org.kurento.client.MediaLatencyStat;
import org.kurento.client.MediaPipeline;
import org.kurento.client.MediaType;
import org.kurento.client.OnIceCandidateEvent;
import org.kurento.client.Properties;
import org.kurento.client.RTCInboundRTPStreamStats;
import org.kurento.client.RTCOutboundRTPStreamStats;
import org.kurento.client.RecorderEndpoint;
import org.kurento.client.Stats;
import org.kurento.client.WebRtcEndpoint;
import org.kurento.jsonrpc.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
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

  private List<MediaElement> sourceMediaElementList = new ArrayList<>();
  private List<MediaElement> targetMediaElementList = new ArrayList<>();

  private Multimap<String, Object> latencies =
      Multimaps.synchronizedListMultimap(ArrayListMultimap.<String, Object>create());
  private Thread latencyThread;
  private ExecutorService executor;

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
    log.info("[WS session {}] Reserving {} points to create source KurentoClient",
        wsSession.getId(), loadPoints);
    Properties properties = new Properties();
    properties.add("loadPoints", loadPoints);
    sourceKurentoClient = KurentoClient.create(properties);
    log.info("[WS session {}] Source KurentoClient {} - {}", wsSession.getId(), sourceKurentoClient,
        sourceKurentoClient.getServerManager());

    log.info("[WS session {}] Reserving {} points to create target KurentoClient",
        wsSession.getId(), loadPoints);
    targetKurentoClient = KurentoClient.create(properties);
    log.info("[WS session {}] Target KurentoClient {} - {}", wsSession.getId(), targetKurentoClient,
        targetKurentoClient.getServerManager());

    // Response
    JsonObject response = new JsonObject();
    response.addProperty("id", "startResponse");
    response.addProperty("response", "accepted");

    // Media pipelines
    sourceMediaPipeline = sourceKurentoClient.createMediaPipeline();
    targetMediaPipeline = targetKurentoClient.createMediaPipeline();

    log.info("[WS session {}] Source MediaPipeline {}", wsSession.getId(), sourceMediaPipeline);
    log.info("[WS session {}] Target MediaPipeline {}", wsSession.getId(), targetKurentoClient);

    sourceWebRtcEndpoint = createWebRtcEndpoint(sourceMediaPipeline, bandwidth);
    sourceWebRtcEndpoint.setName("sourceWebRtcEndpoint");
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
    sourceMediaElementList.add(sourceWebRtcEndpoint);

    int webrtcChannels = jsonMessage.getAsJsonPrimitive("webrtcChannels").getAsInt();
    for (int i = 0; i < webrtcChannels; i++) {
      WebRtcEndpoint webRtcEndpoint1 = createWebRtcEndpoint(sourceMediaPipeline, bandwidth);
      webRtcEndpoint1.setName("sourceWebRtcEndpoint" + i);
      sourceWebRtcEndpoint.connect(webRtcEndpoint1);
      WebRtcEndpoint webRtcEndpoint2 = createWebRtcEndpoint(targetMediaPipeline, bandwidth);
      webRtcEndpoint2.setName("targetWebRtcEndpoint" + i);
      connectWebRtcEndpoints(webRtcEndpoint1, webRtcEndpoint2);
      sourceMediaElementList.add(webRtcEndpoint1);

      RecorderEndpoint recorder =
          new RecorderEndpoint.Builder(targetMediaPipeline, "file:///dev/null").build();
      webRtcEndpoint2.connect(recorder);
      recorder.setName("recorderWebRtcEndpoint" + i);
      recorder.record();
      targetMediaElementList.add(recorder);
      targetMediaElementList.add(webRtcEndpoint2);
    }

    int latencyRate = jsonMessage.getAsJsonPrimitive("latencyRate").getAsInt();
    latencyThread = gatherLatencies(latencyRate);

    // Send response message
    handler.sendMessage(wsSession, new TextMessage(response.toString()));
  }

  private Thread gatherLatencies(final int rateKmsLatency) {
    log.info("[WS session {}] Starting latency gathering (rate {} ms)", wsSession.getId(),
        rateKmsLatency);

    sourceMediaPipeline.setLatencyStats(true);
    targetMediaPipeline.setLatencyStats(true);

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        while (true) {
          try {
            for (int i = 0; i < sourceMediaElementList.size(); i++) {
              final MediaElement w1 = sourceMediaElementList.get(i);
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    Object[] stats = getStats(w1);
                    latencies.put("latency-usec-" + w1.getName(), stats[0]);
                    latencies.put("packetLost-" + w1.getName(), stats[1]);
                    latencies.put("jitter-msec-" + w1.getName(), stats[2]);
                    latencies.put("bytes-sent-" + w1.getName(), stats[3]);
                    latencies.put("bytes-received-" + w1.getName(), stats[4]);
                  } catch (Exception e) {
                    log.info("Exception gathering stats in pipeline #1 {}", e.getMessage());
                  }
                }
              });
            }
            for (int i = 0; i < targetMediaElementList.size(); i++) {
              final MediaElement w2 = targetMediaElementList.get(i);
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    Object[] stats = getStats(w2);
                    latencies.put("latency-usec-" + w2.getName(), stats[0]);
                    latencies.put("packetLost-" + w2.getName(), stats[1]);
                    latencies.put("jitter-msec-" + w2.getName(), stats[2]);
                    latencies.put("bytes-sent-" + w2.getName(), stats[3]);
                    latencies.put("bytes-received-" + w2.getName(), stats[4]);
                  } catch (Exception e) {
                    log.info("Exception gathering stats in pipeline #2 {}", e.getMessage());
                  }
                }
              });
            }
          } catch (Exception e) {
            log.debug("Exception gathering latency {}", e.getMessage());
          } finally {
            try {
              Thread.sleep(rateKmsLatency);
            } catch (InterruptedException e) {
              log.debug("Interrupted thread for gathering videoE2ELatency");
            }
          }
        }
      }
    });
    thread.start();

    return thread;
  }

  public String getCsv(Multimap<String, Object> multimap, boolean orderKeys) throws IOException {
    StringWriter writer = new StringWriter();
    NumberFormat numberFormat = new DecimalFormat("##.###");

    // Header
    boolean first = true;
    Set<String> keySet = orderKeys ? new TreeSet<String>(multimap.keySet()) : multimap.keySet();
    for (String key : keySet) {
      if (!first) {
        writer.append(',');
      }
      writer.append(key);
      first = false;
    }
    writer.append('\n');

    // Values
    int i = 0;
    boolean moreValues;
    do {
      moreValues = false;
      first = true;
      for (String key : keySet) {
        Object[] array = multimap.get(key).toArray();
        moreValues = i < array.length;
        if (moreValues) {
          if (!first) {
            writer.append(',');
          }

          writer.append(numberFormat.format(array[i]));
        }
        first = false;
      }
      i++;
      if (moreValues) {
        writer.append('\n');
      }
    } while (moreValues);

    writer.flush();
    writer.close();

    return writer.toString();
  }

  private Object[] getStats(MediaElement mediaElement) {
    Object[] output = { 0, 0, 0, 0, 0};
    int countStats = output.length;
    Map<String, Stats> stats = mediaElement.getStats(MediaType.VIDEO);
    Collection<Stats> values = stats.values();
    for (Stats s : values) {
      if (s instanceof EndpointStats) {
        List<MediaLatencyStat> e2eLatency = ((EndpointStats) s).getE2ELatency();
        if (!e2eLatency.isEmpty()) {
          output[0] = e2eLatency.get(0).getAvg() / 1000; // microseconds
          countStats--;
        }
      }
      if (s instanceof RTCOutboundRTPStreamStats) {
        output[1] = ((RTCOutboundRTPStreamStats) s).getPacketsLost();
        output[3] = ((RTCOutboundRTPStreamStats) s).getBytesSent();
        countStats = countStats - 2;
      }
      if (s instanceof RTCInboundRTPStreamStats) {
        output[2] = ((RTCInboundRTPStreamStats) s).getJitter() * 1000; // milliseconds
        output[4] = ((RTCInboundRTPStreamStats) s).getBytesReceived();
        countStats = countStats - 2;
      }
      if (countStats == 0) {
        break;
      }
    }
    return output;
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

  public void releaseSession() throws InterruptedException {
    log.info("[WS session {}] Releasing session", wsSession.getId());

    if (latencyThread != null) {
      log.info("[WS session {}] Releasing latencies thread", wsSession.getId());
      executor.shutdownNow();
      latencyThread.interrupt();
    }

    if (sourceMediaPipeline != null) {
      log.info("[WS session {}] Releasing source media pipeline", wsSession.getId());
      sourceMediaPipeline.release();
      sourceMediaPipeline = null;
    }

    if (targetMediaPipeline != null) {
      log.info("[WS session {}] Releasing target media pipeline", wsSession.getId());
      targetMediaPipeline.release();
      targetMediaPipeline = null;
    }

    if (sourceKurentoClient != null) {
      log.info("[WS session {}] Destroying source kurentoClient", wsSession.getId());
      sourceKurentoClient.destroy();
      sourceKurentoClient = null;
    }

    if (targetKurentoClient != null) {
      log.info("[WS session {}] Destroying target kurentoClient", wsSession.getId());
      targetKurentoClient.destroy();
      targetKurentoClient = null;
    }

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

  public Multimap<String, Object> getLatencies() {
    return latencies;
  }

  public String getLatenciesAsCsv() throws IOException {
    return getCsv(latencies, true);
  }

}
