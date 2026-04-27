package replication;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import com.rabbitmq.client.*;


public class DashboardServer {

    private static final int PORT = 8080;
    private static final long TIMEOUT_MS = 5_000;

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/",        new StaticHandler());
        server.createContext("/status",  new StatusHandler());
        server.createContext("/write",   new WriteHandler());
        server.createContext("/readlast",new ReadLastHandler());
        server.createContext("/readall", new ReadAllHandler());
        server.createContext("/reset",   new ResetHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println(" Dashboard running on http://localhost:" + PORT );
       ;
    }


    static void send(HttpExchange ex, int code, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType + "; charset=utf-8");
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    static String readBody(HttpExchange ex) throws IOException {
        return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    }

    static String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"")
                .replace("\n", "\\n").replace("\r", "");
    }


    static class StaticHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            send(ex, 200, "text/html", DASHBOARD_HTML);
        }
    }


    static class StatusHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            boolean[] alive = new boolean[4];  
            try (Connection conn = RabbitConfig.newConnection();
                 Channel ch = conn.createChannel()) {
                for (int i = 1; i <= 3; i++) {
                    try {
                        long consumers = ch.consumerCount(RabbitConfig.READ_QUEUE_PREFIX + i);
                        alive[i] = consumers > 0;
                    } catch (Exception e) {
                        alive[i] = false;
                    }
                }
            } catch (Exception ignored) {}

            StringBuilder sb = new StringBuilder("{\"replicas\":[");
            for (int i = 1; i <= 3; i++) {
                Path file = Paths.get("replica" + i + "/data.txt");
                List<String> lines = new ArrayList<>();
                if (Files.exists(file)) {
                    try { lines = Files.readAllLines(file, StandardCharsets.UTF_8); }
                    catch (IOException ignored) {}
                }
                sb.append("{\"id\":").append(i)
                  .append(",\"online\":").append(alive[i])
                  .append(",\"lines\":[");
                for (int j = 0; j < lines.size(); j++) {
                    sb.append("\"").append(escapeJson(lines.get(j))).append("\"");
                    if (j < lines.size() - 1) sb.append(",");
                }
                sb.append("]}");
                if (i < 3) sb.append(",");
            }
            sb.append("]}");
            send(ex, 200, "application/json", sb.toString());
        }
    }


    static class WriteHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            String line = readBody(ex).trim();
            if (line.isEmpty()) { send(ex, 400, "application/json", "{\"error\":\"empty line\"}"); return; }
            try (Connection conn = RabbitConfig.newConnection();
                 Channel ch = conn.createChannel()) {
                ch.exchangeDeclare(RabbitConfig.WRITE_EXCHANGE, "fanout", true);
                ch.basicPublish(RabbitConfig.WRITE_EXCHANGE, "", null, line.getBytes(StandardCharsets.UTF_8));
                Thread.sleep(300);
                send(ex, 200, "application/json", "{\"ok\":true,\"line\":\"" + escapeJson(line) + "\"}");
            } catch (Exception e) {
                send(ex, 500, "application/json", "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }


    static class ReadLastHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            try (Connection conn = RabbitConfig.newConnection();
                 Channel ch = conn.createChannel()) {

                ch.exchangeDeclare(RabbitConfig.READ_EXCHANGE,  "direct", true);
                ch.exchangeDeclare(RabbitConfig.REPLY_EXCHANGE, "direct", true);

                String replyQueue = ch.queueDeclare().getQueue();
                ch.queueBind(replyQueue, RabbitConfig.REPLY_EXCHANGE, replyQueue);

                BlockingQueue<String> box = new ArrayBlockingQueue<>(1);
                ch.basicConsume(replyQueue, true, (tag, d) -> {
                    box.offer(new String(d.getBody(), StandardCharsets.UTF_8));
                }, tag -> {});

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().replyTo(replyQueue).build();
                byte[] body = RabbitConfig.CMD_READ_LAST.getBytes(StandardCharsets.UTF_8);

                List<Integer> sent = new ArrayList<>();
                for (int i = 1; i <= 3; i++) {
                    try {
                        ch.queueDeclarePassive(RabbitConfig.READ_QUEUE_PREFIX + i);
                        ch.basicPublish(RabbitConfig.READ_EXCHANGE, RabbitConfig.READ_QUEUE_PREFIX + i, props, body);
                        sent.add(i);
                    } catch (Exception ignored) {}
                }

                String reply = box.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
                if (reply == null) reply = "TIMEOUT: no replica responded";

                send(ex, 200, "application/json",
                        "{\"reply\":\"" + escapeJson(reply) + "\",\"sentTo\":" + sent + "}");

            } catch (Exception e) {
                send(ex, 500, "application/json", "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }


    static class ReadAllHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            Map<Integer, List<String>> replicaLines    = new ConcurrentHashMap<>();
            Set<Integer>               finished        = ConcurrentHashMap.newKeySet();

            try (Connection conn = RabbitConfig.newConnection();
                 Channel ch = conn.createChannel()) {

                ch.exchangeDeclare(RabbitConfig.READ_EXCHANGE,  "direct", true);
                ch.exchangeDeclare(RabbitConfig.REPLY_EXCHANGE, "direct", true);

                String replyQueue = ch.queueDeclare().getQueue();
                ch.queueBind(replyQueue, RabbitConfig.REPLY_EXCHANGE, replyQueue);

                List<Integer> live = new ArrayList<>();
                for (int i = 1; i <= 3; i++) {
                    try { ch.queueDeclarePassive(RabbitConfig.READ_QUEUE_PREFIX + i); live.add(i); }
                    catch (Exception ignored) {}
                }

                if (live.isEmpty()) {
                    send(ex, 200, "application/json", "{\"lines\":[],\"replicaLines\":{},\"live\":[]}");
                    return;
                }

                CountDownLatch latch = new CountDownLatch(live.size());
                ch.basicConsume(replyQueue, true, (tag, d) -> {
                    String raw = new String(d.getBody(), StandardCharsets.UTF_8);
                    int sep = raw.indexOf('|');
                    if (sep < 0) return;
                    int    id      = Integer.parseInt(raw.substring(0, sep));
                    String content = raw.substring(sep + 1);
                    if (RabbitConfig.CMD_END_OF_FILE.equals(content)) {
                        if (finished.add(id)) latch.countDown();
                    } else {
                        replicaLines.computeIfAbsent(id, k -> new CopyOnWriteArrayList<>()).add(content);
                    }
                }, tag -> {});

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().replyTo(replyQueue).build();
                byte[] body = RabbitConfig.CMD_READ_ALL.getBytes(StandardCharsets.UTF_8);
                for (int id : live) {
                    ch.basicPublish(RabbitConfig.READ_EXCHANGE, RabbitConfig.READ_QUEUE_PREFIX + id, props, body);
                }

                latch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

                int majority = (live.size() / 2) + 1;
                Map<String, Integer> votes = new LinkedHashMap<>();
                for (Map.Entry<Integer, List<String>> e : replicaLines.entrySet()) {
                    new LinkedHashSet<>(e.getValue()).forEach(l -> votes.merge(l, 1, Integer::sum));
                }

                List<String> result = votes.entrySet().stream()
                        .filter(e -> e.getValue() >= majority)
                        .map(Map.Entry::getKey)
                        .sorted(Comparator.comparingInt(DashboardServer::lineNum))
                        .collect(Collectors.toList());

                StringBuilder sb = new StringBuilder();
                sb.append("{\"majority\":").append(majority)
                  .append(",\"live\":").append(live)
                  .append(",\"lines\":[");
                for (int i = 0; i < result.size(); i++) {
                    sb.append("\"").append(escapeJson(result.get(i))).append("\"");
                    if (i < result.size() - 1) sb.append(",");
                }
                sb.append("],\"votes\":{");
                boolean first = true;
                for (Map.Entry<String, Integer> e : votes.entrySet()) {
                    if (!first) sb.append(",");
                    sb.append("\"").append(escapeJson(e.getKey())).append("\":").append(e.getValue());
                    first = false;
                }
                sb.append("},\"replicaLines\":{");
                for (int i = 1; i <= 3; i++) {
                    List<String> lines = replicaLines.getOrDefault(i, List.of());
                    sb.append("\"").append(i).append("\":[");
                    for (int j = 0; j < lines.size(); j++) {
                        sb.append("\"").append(escapeJson(lines.get(j))).append("\"");
                        if (j < lines.size() - 1) sb.append(",");
                    }
                    sb.append("]");
                    if (i < 3) sb.append(",");
                }
                sb.append("}}");

                send(ex, 200, "application/json", sb.toString());

            } catch (Exception e) {
                send(ex, 500, "application/json", "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
            }
        }
    }

    static int lineNum(String line) {
        try { return Integer.parseInt(line.trim().split("\\s+")[0]); }
        catch (NumberFormatException e) { return Integer.MAX_VALUE; }
    }



    static class ResetHandler implements HttpHandler {
        public void handle(HttpExchange ex) throws IOException {
            for (int i = 1; i <= 3; i++) {
                Path file = Paths.get("replica" + i + "/data.txt");
                Files.deleteIfExists(file);
            }
            send(ex, 200, "application/json", "{\"ok\":true}");
        }
    }

    static final String DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TP3 Replication</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: monospace; background: #f5f5f5; color: #222; padding: 20px; }
  h1 { font-size: 18px; margin-bottom: 4px; }
  .sub { font-size: 12px; color: #888; margin-bottom: 20px; }
  .controls { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-bottom: 20px; background: #fff; padding: 14px; border: 1px solid #ddd; border-radius: 6px; }
  input { border: 1px solid #ccc; padding: 6px 10px; font-family: monospace; font-size: 13px; border-radius: 4px; outline: none; }
  input:focus { border-color: #333; }
  button { padding: 7px 14px; font-family: monospace; font-size: 13px; border: 1px solid #333; border-radius: 4px; background: #fff; cursor: pointer; }
  button:hover { background: #333; color: #fff; }
  button.danger { border-color: #c00; color: #c00; }
  button.danger:hover { background: #c00; color: #fff; }
  .warn { background: #fff8e1; border: 1px solid #f0c000; color: #7a5f00; padding: 8px 12px; border-radius: 4px; font-size: 12px; margin-bottom: 14px; display: none; }
  .warn.show { display: block; }
  .grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 20px; }
  .card { background: #fff; border: 1px solid #ddd; border-radius: 6px; overflow: hidden; }
  .card.inconsistent { border-color: #f0c000; }
  .card.offline { border-color: #c00; opacity: 0.6; }
  .card-head { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; background: #fafafa; border-bottom: 1px solid #ddd; font-size: 13px; font-weight: bold; }
  .card-body { padding: 10px 12px; min-height: 100px; font-size: 12px; }
  .line { padding: 3px 6px; border-radius: 3px; margin-bottom: 2px; }
  .line.ok   { background: #f0fff4; color: #1a7f3c; }
  .line.miss { background: #fff0f0; color: #c00; text-decoration: line-through; }
  .empty { color: #aaa; font-size: 12px; padding: 10px 0; }
  .result { background: #fff; border: 1px solid #ddd; border-radius: 6px; padding: 14px; margin-bottom: 20px; font-size: 13px; }
  .result-title { font-weight: bold; margin-bottom: 8px; font-size: 14px; }
  .vote-row { display: flex; justify-content: space-between; padding: 4px 8px; border-radius: 3px; margin-bottom: 2px; }
  .vote-row.pass { background: #f0fff4; color: #1a7f3c; }
  .vote-row.fail { background: #fff0f0; color: #c00; }
  .log-box { background: #fff; border: 1px solid #ddd; border-radius: 6px; padding: 12px; }
  .log-list { max-height: 140px; overflow-y: auto; font-size: 11px; line-height: 1.8; }
  .log-line { border-bottom: 1px solid #f0f0f0; padding: 1px 0; }
  .log-line .ts { color: #aaa; margin-right: 6px; }
  .log-line.ok  .msg { color: #1a7f3c; }
  .log-line.err .msg { color: #c00; }
  .log-line.info .msg { color: #0066cc; }
  label { font-size: 11px; color: #888; display: block; margin-bottom: 3px; }
  .field { display: flex; flex-direction: column; }
  .sep { color: #ccc; }
</style>
</head>
<body>

<h1>TP3 Replication</h1>

<div class="controls">
  <div class="field">
    <label>line number</label>
    <input id="lineNum" type="text" value="1" style="width:50px">
  </div>
  <div class="field">
    <label>TEXT</label>
    <input id="lineText" type="text" value="Texte message" style="width:200px">
  </div>
  <button onclick="doWrite()">Write</button>
  <span class="sep">|</span>
  <button onclick="doReadLast()">Read Last</button>
  <button onclick="doReadAll()">Majority Vote</button>
  <span class="sep">|</span>
  <button class="danger" onclick="doReset()">Reset</button>
</div>

<div class="warn" id="warn"> Inconsistency detected — replicas have different content. Run Majority Vote to recover.</div>

<div class="grid">
  <div class="card" id="card1">
    <div class="card-head"><span>Replica 1</span><span id="s1" style="font-size:11px;color:#1a7f3c">ONLINE</span></div>
    <div class="card-body" id="b1"><div class="empty">No data yet</div></div>
  </div>
  <div class="card" id="card2">
    <div class="card-head"><span>Replica 2</span><span id="s2" style="font-size:11px;color:#1a7f3c">ONLINE</span></div>
    <div class="card-body" id="b2"><div class="empty">No data yet</div></div>
  </div>
  <div class="card" id="card3">
    <div class="card-head"><span>Replica 3</span><span id="s3" style="font-size:11px;color:#1a7f3c">ONLINE</span></div>
    <div class="card-body" id="b3"><div class="empty">No data yet</div></div>
  </div>
</div>

<div class="result" id="resultBox">
  <div class="result-title" id="resultTitle">Result</div>
  <div id="resultContent" style="color:#aaa">No operation run yet.</div>
</div>

<div class="log-box">
  <div style="font-size:11px;font-weight:bold;color:#888;margin-bottom:8px;letter-spacing:1px">LOG</div>
  <div class="log-list" id="logList">
    <div class="log-line info"><span class="ts">--:--:--</span><span class="msg">Dashboard ready.</span></div>
  </div>
</div>

<script>
  let allLines = [];

  async function poll() {
    try {
      const d = await fetch('/status').then(r => r.json());
      renderReplicas(d.replicas);
    } catch(e) {}
  }

  setInterval(poll, 1500);
  poll();

  function renderReplicas(replicas) {
    allLines = [...new Set(replicas.flatMap(r => r.lines))].sort((a,b) => ln(a)-ln(b));
    const lengths = replicas.map(r => r.lines.length);
    const inconsistent = new Set(lengths).size > 1;
    document.getElementById('warn').classList.toggle('show', inconsistent);

    replicas.forEach(r => {
      const body  = document.getElementById('b'  + r.id);
      const card  = document.getElementById('card' + r.id);
      const label = document.getElementById('s'  + r.id);

      if (r.online) {
        label.textContent = 'ONLINE';
        label.style.color = '#156d33';
      } else {
        label.textContent = 'OFFLINE';
        label.style.color = 'rgb(137, 6, 6)';
      }

      if (r.lines.length === 0) { body.innerHTML = '<div class="empty">No data yet</div>'; }
      else {
        body.innerHTML = allLines.map(line => {
          const has = r.lines.includes(line);
          return `<div class="line ${has?'ok':'miss'}">${line}${!has?' &nbsp;<b>MISSING</b>':''}</div>`;
        }).join('');
      }

      if (!r.online) card.className = 'card offline';
      else if (inconsistent && r.lines.length < allLines.length) card.className = 'card inconsistent';
      else card.className = 'card';
    });
  }

  async function doWrite() {
    const num = document.getElementById('lineNum').value.trim();
    const txt = document.getElementById('lineText').value.trim();
    if (!num || !txt) return;
    const line = num + ' ' + txt;
    addLog('info', 'Writing: "' + line + '"');
    const d = await fetch('/write', {method:'POST', body:line}).then(r=>r.json());
    if (d.ok) { addLog('ok', 'Written: "' + line + '"'); document.getElementById('lineNum').value = parseInt(num)+1; }
    else addLog('err', d.error);
    await poll();
  }

  async function doReadLast() {
    addLog('info', 'READ_LAST sent...');
    const d = await fetch('/readlast', {method:'POST'}).then(r=>r.json());
    if (d.reply) {
      addLog('ok', d.reply);
      setResult('Read Last', `<div class="vote-row pass"><span>${d.reply}</span><span>FIRST REPLY</span></div>`);
    } else addLog('err', d.error || 'timeout');
  }

  async function doReadAll() {
    addLog('info', 'READ_ALL + majority vote...');
    const d = await fetch('/readall', {method:'POST'}).then(r=>r.json());
    if (d.lines !== undefined) {
      addLog('ok', d.lines.length + ' lines passed majority (' + d.majority + '/' + d.live.length + ')');
      const rows = Object.entries(d.votes).sort((a,b)=>ln(a[0])-ln(b[0])).map(([line,cnt]) => {
        const pass = cnt >= d.majority;
        return `<div class="vote-row ${pass?'pass':'fail'}"><span>${line}</span><span>${cnt}/${d.live.length} · ${pass?'KEPT':'REJECTED'}</span></div>`;
      }).join('');
      setResult('Majority Vote — threshold: ' + d.majority + '/' + d.live.length, rows || '<span style="color:#aaa">No data.</span>');
    } else addLog('err', d.error);
  }

  async function doReset() {
    if (!confirm('Reset all replica files?')) return;
    await fetch('/reset', {method:'POST'});
    allLines = [];
    document.getElementById('lineNum').value = '1';
    document.getElementById('warn').classList.remove('show');
    setResult('Reset', '<span style="color:#aaa">All files cleared.</span>');
    addLog('ok', 'Reset done.');
    await poll();
  }

  function setResult(title, html) {
    document.getElementById('resultTitle').textContent = title;
    document.getElementById('resultContent').innerHTML = html;
  }

  function addLog(type, msg) {
    const ts = new Date().toLocaleTimeString();
    const el = document.createElement('div');
    el.className = 'log-line ' + type;
    el.innerHTML = `<span class="ts">${ts}</span><span class="msg">${msg}</span>`;
    const list = document.getElementById('logList');
    list.prepend(el);
    if (list.children.length > 40) list.lastChild.remove();
  }

  function ln(line) { const n = parseInt((line||'').trim().split(' ')[0]); return isNaN(n)?9999:n; }

  document.getElementById('lineText').addEventListener('keydown', e => { if(e.key==='Enter') doWrite(); });
  document.getElementById('lineNum').addEventListener('keydown',  e => { if(e.key==='Enter') doWrite(); });
</script>
</body>
</html>

""";
}