"""Generates the framework flowcharts as one draw.io file (one page per phase) and one PNG per page.

Both outputs come from the same definitions below, so the PNGs always match the .drawio source.
  python make_flowcharts.py
Requires Pillow for the PNGs. Open framework_flowcharts.drawio at https://app.diagrams.net or in draw.io desktop.
"""
import os
import textwrap
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont

HERE = os.path.dirname(os.path.abspath(__file__))
STYLE = {  # fill, stroke, drawio shape
    "start": ("#d5e8d4", "#82b366", "ellipse"),
    "end": ("#d5e8d4", "#82b366", "ellipse"),
    "proc": ("#dae8fc", "#6c8ebf", "rounded"),
    "dec": ("#fff2cc", "#d6b656", "rhombus"),
    "side": ("#f8cecc", "#b85450", "rounded"),
    "db": ("#e1d5e7", "#9673a6", "rounded"),
    "node": ("#ffe6cc", "#d79b00", "rounded"),
    "ext": ("#f5f5f5", "#666666", "rounded"),
}


class Page:
    def __init__(self, name, w, h):
        self.name, self.w, self.h = name, w, h
        self.nodes, self.edges = {}, []

    def node(self, nid, x, y, w, h, text, kind="proc"):
        self.nodes[nid] = (x, y, w, h, text, kind)

    def edge(self, a, b, label="", fs="b", ts="t"):
        self.edges.append((a, b, label, fs, ts))


def flow(name, title, steps):
    """Vertical flowchart. A step is (kind, text) or (kind, text, down_label, side_label, side_text) for a decision."""
    W, GAP = 360, 46
    heights = {"start": 56, "end": 56, "proc": 62, "dec": 92}
    p = Page(name, 1120, 80 + sum(heights[s[0]] + GAP for s in steps))
    p.node("title", 20, 14, 700, 36, title, "ext")
    y, prev = 70, None
    for i, s in enumerate(steps):
        kind, text = s[0], s[1]
        h = heights[kind]
        p.node(f"s{i}", 120, y, W, h, text, kind)
        if prev:
            label = steps[int(prev[1:])][2] if steps[int(prev[1:])][0] == "dec" else ""
            p.edge(prev, f"s{i}", label)
        if kind == "dec":
            p.node(f"x{i}", 620, y + (h - 70) // 2, 440, 70, s[4], "side")
            p.edge(f"s{i}", f"x{i}", s[3], "r", "l")
        prev, y = f"s{i}", y + h + GAP
    return p


pages = []

# ------------------------------------------------------------------ architecture
a = Page("1 Architecture", 1120, 720)
a.node("title", 20, 14, 800, 36, "Distributed object storage framework: components", "ext")
a.node("ui", 60, 74, 300, 70, "Clients: React web UI (/ui), curl, test scripts", "ext")
a.node("gw", 60, 200, 520, 150, "Gateway service (FastAPI): S3-like API, chunking (fixed / content-defined), SHA-256 chunk ids, "
       "consistent hash ring (128 virtual nodes), replication with quorums W/R, erasure coding (zfec), dedup + refcounts, search, cluster status", "proc")
a.node("pg", 760, 200, 300, 150, "Postgres: buckets, objects, chunks + reference counts, chunk locations, nodes, repair log", "db")
a.node("bg", 420, 66, 330, 96, "Background jobs inside the gateway: failure detection, repair, scrubber, garbage collection, rebalance", "dec")
for i in range(4):
    a.node(f"n{i}", 40 + i * 270, 470, 240, 110, f"Storage node {i + 1} (FastAPI)\nPUT / GET / HEAD / DELETE chunks\nverify SHA-256 on every read\nown volume /data", "node")
a.node("env", 40, 630, 1020, 54, "Environment: Docker + Kubernetes, Prometheus, Grafana, Kafka, RabbitMQ, Python venv, GitHub", "ext")
a.edge("ui", "gw", "HTTP", "b", "t")
a.edge("gw", "pg", "metadata (SQL)", "r", "l")
a.edge("bg", "gw", "", "b", "t")
for i in range(4):
    a.edge("gw", f"n{i}", "chunks, shards, heartbeats" if i == 0 else "", "b", "t")
pages.append(a)

# ------------------------------------------------------------------ write path
pages.append(flow("2 Write path", "Write path: PUT /bucket/key", [
    ("start", "Client: PUT /bucket/key with the object bytes"),
    ("proc", "Gateway: bucket exists? read its durability policy (N, W, R or erasure coding k+m)"),
    ("proc", "Split into chunks: fixed 4 MiB or content-defined (Gear hash)"),
    ("proc", "Chunk id = SHA-256 of the chunk; lock the ids (advisory locks)"),
    ("dec", "Chunk already stored?", "no: new chunk", "yes", "Deduplicated: only the reference count is increased, nothing is written (then next chunk)"),
    ("proc", "Hash ring picks the target nodes: N nodes, or k+m nodes for erasure coding"),
    ("proc", "Replication: PUT the chunk to N nodes.  Erasure coding: zfec encodes k+m shards, one shard per node"),
    ("dec", "Write quorum met? (>= W copies, or >= k+1 shards)", "yes", "no", "503: too many nodes down; roll back, written pieces go to the garbage-collection queue"),
    ("proc", "Postgres (one transaction): chunk rows with reference counts, chunk locations"),
    ("proc", "Insert the object metadata and the ordered list of its chunks"),
    ("end", "Return 200 with the ETag (SHA-256 of the object)"),
]))

# ------------------------------------------------------------------ read path
pages.append(flow("3 Read path", "Read path: GET /bucket/key (optional Range)", [
    ("start", "Client: GET /bucket/key, optionally with a Range header"),
    ("proc", "Gateway loads the manifest from Postgres: ordered chunks, locations, node health"),
    ("proc", "Select the chunks that overlap the requested byte range"),
    ("dec", "Chunk replicated or erasure coded?", "replicated", "erasure coded", "Fetch k shards from live nodes (data shards first), verify each shard hash, decode with zfec if a data shard is missing"),
    ("proc", "Fetch the chunk from one healthy replica"),
    ("dec", "Read quorum met? (R-1 other replicas confirm with HEAD)", "yes", "no", "503 read quorum not met (header X-DFS-Consistency: any allows a single-replica read)"),
    ("dec", "SHA-256 of the chunk equals its id?", "yes", "no", "Try the next replica or shard; when none is left return 502"),
    ("proc", "Concatenate the chunks and cut the requested range"),
    ("end", "Return 200 (or 206 with Content-Range)"),
]))

# ------------------------------------------------------------------ failure detection and repair
pages.append(flow("4 Failure and repair", "Failure detection and automatic repair", [
    ("start", "Every 5 s each node sends a heartbeat (id, free space, chunk count)"),
    ("proc", "Gateway stores last_seen in Postgres"),
    ("dec", "Heartbeat missing for 15 s?", "yes", "no", "Node is alive: nothing to do"),
    ("proc", "Node marked FAILED (shown by /nodes, the UI and the repair log)"),
    ("proc", "Repair job (every 10 s): find chunks with fewer healthy copies or shards than required"),
    ("dec", "Chunk replicated or erasure coded?", "replicated", "erasure coded", "Read k surviving shards, decode with zfec, regenerate the lost shard"),
    ("proc", "Copy a healthy replica to a remaining node chosen by the hash ring"),
    ("proc", "Update locations in Postgres; queue the failed node's old copies for garbage collection"),
    ("end", "Log a repair-job entry: chunks rebuilt, bytes read and written"),
]))

# ------------------------------------------------------------------ scrubbing
pages.append(flow("5 Scrubbing", "Background scrubber (integrity)", [
    ("start", "Every 300 s (or POST /admin/scrub) the gateway asks every live node to scrub"),
    ("proc", "Node re-reads every chunk file and recomputes its SHA-256"),
    ("dec", "Hash equals the file name?", "yes", "no", "Bit rot: move the file to quarantine and report it as corrupted"),
    ("proc", "Gateway compares the node's file list with the locations recorded in Postgres"),
    ("proc", "Corrupted and missing copies are forgotten (location rows removed)"),
    ("proc", "Repair job rebuilds them from a healthy copy (replica copy, or zfec reconstruction)"),
    ("end", "Log scrub-corrupt, scrub-missing and reconstruct events"),
]))

# ------------------------------------------------------------------ rebalancing
pages.append(flow("6 Rebalancing", "Adding or removing a node (consistent hashing)", [
    ("start", "A node joins the ring (first heartbeat) or is decommissioned"),
    ("proc", "Hash ring recomputed: only keys next to the node's virtual positions change owner (about 1/N)"),
    ("proc", "POST /admin/rebalance (or decommission): compare every chunk's copies with the ring's choice"),
    ("dec", "Copy is on the wrong node?", "yes", "no", "Leave it where it is"),
    ("proc", "Copy to the new owner, verify the hash, update Postgres"),
    ("proc", "Remove the old copy (garbage collection)"),
    ("end", "Report the fraction of copies moved (about 22% for 4 to 5 nodes, against 84% for hash mod N)"),
]))


# ================================================================== drawio export
def anchor(side):
    return {"t": (0.5, 0), "b": (0.5, 1), "l": (0, 0.5), "r": (1, 0.5)}[side]


def drawio(pages):
    out = ['<mxfile host="app.diagrams.net">']
    for pi, pg in enumerate(pages):
        out.append(f'<diagram id="page{pi}" name="{escape(pg.name)}"><mxGraphModel dx="1200" dy="800" grid="1" gridSize="10" '
                   f'page="1" pageWidth="{pg.w}" pageHeight="{pg.h}"><root><mxCell id="0"/><mxCell id="1" parent="0"/>')
        for nid, (x, y, w, h, text, kind) in pg.nodes.items():
            fill, stroke, shape = STYLE[kind]
            shape_style = {"ellipse": "ellipse;", "rhombus": "rhombus;", "rounded": "rounded=1;"}[shape]
            font = "fontSize=16;fontStyle=1;strokeColor=none;fillColor=none;align=left;" if nid == "title" else ""
            style = (f"{shape_style}whiteSpace=wrap;html=1;{font}" if nid == "title"
                     else f"{shape_style}whiteSpace=wrap;html=1;fillColor={fill};strokeColor={stroke};fontSize=12;")
            out.append(f'<mxCell id="{pi}_{nid}" value="{escape(text).replace(chr(10), "&lt;br&gt;")}" style="{style}" vertex="1" parent="1">'
                       f'<mxGeometry x="{x}" y="{y}" width="{w}" height="{h}" as="geometry"/></mxCell>')
        for ei, (a_, b_, label, fs, ts) in enumerate(pg.edges):
            (ex, ey), (nx, ny) = anchor(fs), anchor(ts)
            style = (f"edgeStyle=orthogonalEdgeStyle;rounded=0;html=1;endArrow=block;endFill=1;fontSize=11;"
                     f"exitX={ex};exitY={ey};entryX={nx};entryY={ny};")
            out.append(f'<mxCell id="{pi}_e{ei}" value="{escape(label)}" style="{style}" edge="1" parent="1" source="{pi}_{a_}" target="{pi}_{b_}">'
                       f'<mxGeometry relative="1" as="geometry"/></mxCell>')
        out.append("</root></mxGraphModel></diagram>")
    out.append("</mxfile>")
    return "\n".join(out)


# ================================================================== PNG export
def font(size, bold=False):
    for path in (r"C:\Windows\Fonts\arialbd.ttf" if bold else r"C:\Windows\Fonts\arial.ttf",
                 "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf" if bold else "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"):
        if os.path.exists(path):
            return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def png(pg, path, scale=2):
    img = Image.new("RGB", (pg.w * scale, pg.h * scale), "white")
    d = ImageDraw.Draw(img)
    f12, f11, f16 = font(12 * scale), font(11 * scale), font(16 * scale, True)

    def pt(nid, side):
        x, y, w, h, *_ = pg.nodes[nid]
        ax, ay = anchor(side)
        return ((x + ax * w) * scale, (y + ay * h) * scale)

    for a_, b_, label, fs, ts in pg.edges:
        (x1, y1), (x2, y2) = pt(a_, fs), pt(b_, ts)
        if fs == "r" and ts == "b":
            pts = [(x1, y1), (x2, y1), (x2, y2)]
        elif fs in "tb" and ts in "tb" and abs(x1 - x2) > 2:
            ym = (y1 + y2) / 2
            pts = [(x1, y1), (x1, ym), (x2, ym), (x2, y2)]
        else:
            pts = [(x1, y1), (x2, y2)]
        d.line(pts, fill="#444444", width=2 * scale // 2)
        (xa, ya), (xb, yb) = pts[-2], pts[-1]
        dx, dy = xb - xa, yb - ya
        n = max((dx * dx + dy * dy) ** 0.5, 1)
        ux, uy = dx / n, dy / n
        s = 9 * scale
        d.polygon([(xb, yb), (xb - ux * s - uy * s * 0.5, yb - uy * s + ux * s * 0.5),
                   (xb - ux * s + uy * s * 0.5, yb - uy * s - ux * s * 0.5)], fill="#444444")
        if label:
            lx, ly = (x1 + x2) / 2, (y1 + y2) / 2
            tw = d.textlength(label, font=f11)
            if fs in "tb" and ts in "tb":
                lx, ly = x1 + 6 * scale + tw / 2, y1 + 10 * scale
            d.text((lx - tw / 2, ly - 8 * scale), label, font=f11, fill="#222222")

    for nid, (x, y, w, h, text, kind) in pg.nodes.items():
        fill, stroke, shape = STYLE[kind]
        x, y, w, h = x * scale, y * scale, w * scale, h * scale
        if nid == "title":
            d.text((x, y + 6 * scale), text, font=f16, fill="#1d2733")
            continue
        if shape == "ellipse":
            d.ellipse([x, y, x + w, y + h], fill=fill, outline=stroke, width=2 * scale // 2)
        elif shape == "rhombus":
            d.polygon([(x + w / 2, y), (x + w, y + h / 2), (x + w / 2, y + h), (x, y + h / 2)], fill=fill, outline=stroke)
            d.line([(x + w / 2, y), (x + w, y + h / 2), (x + w / 2, y + h), (x, y + h / 2), (x + w / 2, y)], fill=stroke, width=2 * scale // 2)
        else:
            d.rounded_rectangle([x, y, x + w, y + h], radius=10 * scale, fill=fill, outline=stroke, width=2 * scale // 2)
        inner = w * (0.62 if shape == "rhombus" else 0.9 if shape == "ellipse" else 0.94)
        lines = []
        for para in text.split("\n"):
            cur = ""
            for word in para.split():
                trial = (cur + " " + word).strip()
                if d.textlength(trial, font=f12) <= inner:
                    cur = trial
                else:
                    lines.append(cur)
                    cur = word
            lines.append(cur)
        lh = 16 * scale
        ty = y + (h - lh * len(lines)) / 2
        for ln in lines:
            d.text((x + (w - d.textlength(ln, font=f12)) / 2, ty), ln, font=f12, fill="#1d2733")
            ty += lh
    img.save(path)


if __name__ == "__main__":
    with open(os.path.join(HERE, "framework_flowcharts.drawio"), "w", encoding="utf8") as f:
        f.write(drawio(pages))
    for pg in pages:
        fname = pg.name.lower().replace(" ", "_") + ".png"
        png(pg, os.path.join(HERE, fname))
        print("wrote", fname)
    print("wrote framework_flowcharts.drawio")
