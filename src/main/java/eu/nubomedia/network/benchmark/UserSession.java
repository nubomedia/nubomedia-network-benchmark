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
      webRtcEndpoint1.setName("sourceWebRtcEndpoint" + i);
      sourceWebRtcEndpoint.connect(webRtcEndpoint1);
      WebRtcEndpoint webRtcEndpoint2 = createWebRtcEndpoint(targetMediaPipeline, bandwidth);
      webRtcEndpoint2.setName("targetWebRtcEndpoint" + i);
      connectWebRtcEndpoints(webRtcEndpoint1, webRtcEndpoint2);
      sourceMediaElementList.add(webRtcEndpoint1);

      RecorderEndpoint recorder = new RecorderEndpoint.Builder(targetMediaPipeline,
          "file:///tmp/" + webRtcEndpoint2.getName() + ".webm").build();
      webRtcEndpoint2.connect(recorder);
      recorder.setName(webRtcEndpoint2.getName());
      recorder.record();
      targetMediaElementList.add(recorder);
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
        executor = Executors.newFixedThreadPool(6 * sourceMediaElementList.size());

        while (true) {
          try {
            for (int i = 0; i < sourceMediaElementList.size(); i++) {
              final MediaElement w1 = sourceMediaElementList.get(i);
              final MediaElement w2 = targetMediaElementList.get(i);

              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    latencies.put("latency-usec-" + w1.getName(), getVideoE2ELatency(w1));
                  } catch (Exception e) {
                    log.debug("Exception gathering latency in pipeline #1 {}", e.getMessage());
                  }
                }
              });
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    latencies.put("packetLost-" + w1.getName(), getPacketsLost(w1));
                  } catch (Exception e) {
                    log.debug("Exception gathering packetLost in pipeline #1 {}", e.getMessage());
                  }
                }
              });
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    latencies.put("jitter-usec-" + w1.getName(), getJitter(w1));
                  } catch (Exception e) {
                    log.debug("Exception gathering jitter in pipeline #1 {}", e.getMessage());
                  }
                }
              });
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    latencies.put("latency-usec-" + w2.getName(), getVideoE2ELatency(w2));
                  } catch (Exception e) {
                    log.debug("Exception gathering latency in pipeline #2 {}", e.getMessage());
                  }
                }
              });
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    latencies.put("packetLost-" + w2.getName(), getPacketsLost(w2));
                  } catch (Exception e) {
                    log.debug("Exception gathering packetLost in pipeline #2 {}", e.getMessage());
                  }
                }
              });
              executor.execute(new Runnable() {
                @Override
                public void run() {
                  try {
                    latencies.put("jitter-usec-" + w2.getName(), getJitter(w2));
                  } catch (Exception e) {
                    log.debug("Exception gathering jitter in pipeline #2 {}", e.getMessage());
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

  protected double getVideoE2ELatency(MediaElement mediaElement) {
    Map<String, Stats> stats = mediaElement.getStats(MediaType.VIDEO);
    Collection<Stats> values = stats.values();
    for (Stats s : values) {
      if (s instanceof EndpointStats) {
        List<MediaLatencyStat> e2eLatency = ((EndpointStats) s).getE2ELatency();
        if (!e2eLatency.isEmpty()) {
          return e2eLatency.get(0).getAvg() / 1000; // microseconds
        }
      }
    }
    return 0;
  }

  protected long getPacketsLost(MediaElement mediaElement) {
    Map<String, Stats> stats = mediaElement.getStats(MediaType.VIDEO);
    Collection<Stats> values = stats.values();
    for (Stats s : values) {
      if (s instanceof RTCOutboundRTPStreamStats) {
        return ((RTCOutboundRTPStreamStats) s).getPacketsLost();
      }

    }
    return 0;
  }

  protected double getJitter(MediaElement mediaElement) {
    Map<String, Stats> stats = mediaElement.getStats(MediaType.VIDEO);
    Collection<Stats> values = stats.values();
    for (Stats s : values) {
      if (s instanceof RTCInboundRTPStreamStats) {
        return ((RTCInboundRTPStreamStats) s).getJitter() / 1000; // microseconds
      }
    }
    return 0;
  }

  protected double getVideoInputLatency(MediaElement mediaElement) {
    Map<String, Stats> stats = mediaElement.getStats(MediaType.VIDEO);
    Collection<Stats> values = stats.values();
    for (Stats s : values) {
      if (s instanceof EndpointStats) {
        List<MediaLatencyStat> inputLatency = ((EndpointStats) s).getInputLatency();
        if (!inputLatency.isEmpty()) {
          return inputLatency.get(0).getAvg() / 1000; // microseconds
        }
      }
    }
    return 0;
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
    String emptyLine = "";
    for (int i = 0; i < 6 * sourceMediaElementList.size(); i++) {
      if (i != 0) {
        emptyLine += ",";
      }
      emptyLine += "0";
    }
    emptyLine += "\n";
    String csv = getCsv(latencies, true);
    return csv.replaceAll(emptyLine, "");
  }

}
