from __future__ import annotations

import json
from typing import Any, Dict, List


def build_pixel_transition_html(views: List[Dict[str, Any]]) -> str:
    payload = json.dumps(views)
    return f"""
<!doctype html>
<html lang="en" data-theme="system" data-transition="outIn">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Reddit+Mono:wght@200..900&display=swap');
      :root {{
        --duration: 0.3;
        --size: 12;
        --speed: calc(var(--duration) * 1s);
        --ms: calc(1vmax * var(--size));
      }}
      body {{
        margin: 0;
        padding: 0;
        font-family: 'Reddit Mono', monospace;
        background: #f5f5f5;
        color: #0f1117;
      }}
      .stage {{
        width: 100%;
        display: flex;
        justify-content: center;
        padding: 10px 0;
      }}
      .shell {{
        width: min(900px, 94vw);
        border: 1px solid rgba(255, 255, 255, 0.2);
        border-radius: 16px;
        padding: 1rem;
        background: rgba(255, 255, 255, 0.03);
      }}
      .controls {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
        align-items: center;
        margin-bottom: 1rem;
      }}
      .controls label {{
        font-size: 0.8rem;
        opacity: 0.8;
      }}
      .view {{
        min-height: 320px;
        border: 1px dashed rgba(255, 255, 255, 0.25);
        border-radius: 12px;
        padding: 1.2rem;
        display: grid;
        gap: 0.75rem;
      }}
      h2 {{
        margin: 0;
        text-transform: uppercase;
      }}
      p {{
        margin: 0;
        opacity: 0.9;
        line-height: 1.45;
      }}
      ul {{
        margin: 0;
        padding-left: 1rem;
      }}
      button {{
        border: 1px solid rgba(255,255,255,0.3);
        background: transparent;
        color: inherit;
        border-radius: 8px;
        padding: 0.45rem 0.75rem;
        cursor: pointer;
      }}
      .nav {{
        display: flex;
        justify-content: space-between;
        margin-top: 1rem;
      }}
      ::view-transition-old(root),
      ::view-transition-new(root) {{
        mask-image: var(--mi);
        mask-repeat: no-repeat;
        mask-size: calc(var(--ms) + 1px) calc(var(--ms) + 1px);
        mask-position: var(--mp, -100% -100%);
      }}
      [data-transition='outIn']::view-transition-old(root) {{
        animation: maskOut var(--speed) ease forwards reverse;
      }}
      [data-transition='outIn']::view-transition-new(root) {{
        animation: maskIn var(--speed) calc(var(--speed) * 1.1) ease forwards;
      }}
    </style>
    <style id="vt"></style>
  </head>
  <body>
    <div class="stage">
    <div class="shell">
      <div class="controls">
        <label>Cells <input id="cells" type="range" min="3" max="21" step="2" value="11" /></label>
        <label>Speed <input id="speed" type="range" min="0.2" max="1" step="0.01" value="0.3" /></label>
      </div>
      <div id="view" class="view"></div>
      <div class="nav">
        <button id="prev">Previous</button>
        <button id="next">Next</button>
      </div>
    </div>
    </div>

    <script>
      const views = {payload};
      let index = 0;
      const view = document.getElementById('view');
      let frameTimer = null;
      let frameRaf = 0;

      const shuffleArray = (arr) => {{
        for (let i = arr.length - 1; i > 0; i--) {{
          const j = Math.floor(Math.random() * (i + 1));
          [arr[i], arr[j]] = [arr[j], arr[i]];
        }}
        return arr;
      }};

      const getPositions = (frame, positions, cells) => {{
        const slices = [];
        for (let i = 0; i < cells; i++) {{
          if (i < frame) slices.push(positions.slice(i * cells, (i + 1) * cells));
          else slices.push(positions.slice(frame * cells, (frame + 1) * cells));
        }}
        return slices.join(',');
      }};

      const getFrames = (positions, cells) => {{
        let frames = '';
        const shuffled = shuffleArray(positions.slice());
        for (let f = 1; f < cells; f++) {{
          const sineFrame = Math.floor(Math.sin((f / cells) * (Math.PI / 2)) * 100);
          frames += `${{sineFrame}}% {{ --mp: ${{getPositions(f, shuffled, cells)}}; }}`;
        }}
        frames += `100% {{ --mp: ${{positions.join(',')}}; }}`;
        return frames;
      }};

      const genStyles = () => {{
        const cells = Number(document.getElementById('cells').value);
        const positions = [];
        const mid = Math.ceil(cells * 0.5);
        for (let p = 0; p < Math.pow(cells, 2); p++) {{
          const x = p % cells;
          const y = Math.floor(p / cells);
          const xm = x + 1 - mid;
          const ym = y + 1 - mid;
          positions.push(`calc(50% + (var(--ms) * ${{xm}})) calc(50% + (var(--ms) * ${{ym}}))`);
        }}
        const maskIn = `@keyframes maskIn {{${{getFrames(positions, cells)}}}}`;
        const maskOut = `@keyframes maskOut {{${{getFrames(positions, cells)}}}}`;
        document.querySelector('#vt').innerHTML = `
          :root {{
            --mi: ${{new Array(Math.pow(cells, 2)).fill('linear-gradient(#fff 0 0)').join(',')}};
            --size: ${{Math.ceil(100 / cells)}};
          }}
          ${{maskIn}}
          ${{maskOut}}
        `;
      }};

      const postFrameHeight = () => {{
        const shell = document.querySelector('.shell');
        if (!shell || !window.parent) return;
        const height = Math.ceil(shell.getBoundingClientRect().height + 24);
        window.parent.postMessage({{ type: 'streamlit:setFrameHeight', height }}, '*');
      }};

      const scheduleFrameHeight = (delay = 0) => {{
        if (frameTimer) clearTimeout(frameTimer);
        frameTimer = setTimeout(postFrameHeight, delay);
      }};

      const requestFrameHeight = () => {{
        if (frameRaf) cancelAnimationFrame(frameRaf);
        frameRaf = requestAnimationFrame(postFrameHeight);
      }};

      const attachAutoHeight = () => {{
        const shell = document.querySelector('.shell');
        if (!shell) return;
        if (typeof ResizeObserver !== 'undefined') {{
          const ro = new ResizeObserver(() => requestFrameHeight());
          ro.observe(shell);
          ro.observe(document.body);
        }}
        if (typeof MutationObserver !== 'undefined') {{
          const mo = new MutationObserver(() => requestFrameHeight());
          mo.observe(shell, {{ childList: true, subtree: true, characterData: true }});
        }}
        window.addEventListener('load', () => scheduleFrameHeight(30));
      }};

      const render = () => {{
        const item = views[index] || {{}};
        const bullets = (item.bullets || []).map((b) => `<li>${{b}}</li>`).join('');
        view.innerHTML = `
          <h2>${{item.title || ''}}</h2>
          <p>${{item.subtitle || ''}}</p>
          <p>${{item.body || ''}}</p>
          <ul>${{bullets}}</ul>
          <p>Step ${{index + 1}} / ${{views.length}}</p>
        `;
        scheduleFrameHeight(20);
      }};

      const transitionTo = (nextIndex) => {{
        const apply = () => {{
          index = (nextIndex + views.length) % views.length;
          render();
          scheduleFrameHeight(60);
        }};
        if (!document.startViewTransition) {{
          apply();
          return;
        }}
        document.startViewTransition(apply).finished.then(() => scheduleFrameHeight(80));
      }};

      document.getElementById('prev').addEventListener('click', () => transitionTo(index - 1));
      document.getElementById('next').addEventListener('click', () => transitionTo(index + 1));
      document.getElementById('cells').addEventListener('input', () => {{
        genStyles();
        scheduleFrameHeight(30);
      }});
      document.getElementById('speed').addEventListener('input', (e) => {{
        document.documentElement.style.setProperty('--duration', e.target.value);
        scheduleFrameHeight(30);
      }});
      window.addEventListener('resize', () => scheduleFrameHeight(60));

      attachAutoHeight();
      genStyles();
      render();
      scheduleFrameHeight(120);
    </script>
  </body>
</html>
"""


def build_pixel_transition_geo_html(
    views: List[Dict[str, Any]], crack_points: List[Dict[str, Any]]
) -> str:
    payload = json.dumps(views)
    points_payload = json.dumps(crack_points)
    return f"""
<!doctype html>
<html lang="en" data-theme="system" data-transition="outIn">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Reddit+Mono:wght@200..900&display=swap');
      :root {{
        --duration: 0.36;
        --size: 11;
        --speed: calc(var(--duration) * 1s);
        --ms: calc(1vmax * var(--size));
        --bg: #f5f5f5;
        --ink: #0f1117;
        --line: rgba(15, 17, 23, 0.2);
      }}
      * {{ box-sizing: border-box; }}
      body {{
        margin: 0;
        padding: 0;
        font-family: 'Reddit Mono', monospace;
        background: var(--bg);
        color: var(--ink);
      }}
      .stage {{
        width: 100%;
        display: flex;
        justify-content: center;
        padding: 10px 0;
      }}
      .shell {{
        width: min(1060px, 95vw);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 1rem;
        background: var(--bg);
      }}
      .controls {{
        display: flex;
        flex-wrap: wrap;
        gap: 0.85rem;
        align-items: center;
        margin-bottom: 1rem;
      }}
      .controls label {{
        font-size: 0.78rem;
        opacity: 0.8;
      }}
      .view {{
        min-height: 460px;
        border: 1px dashed var(--line);
        border-radius: 14px;
        padding: 1.2rem;
        display: grid;
        gap: 0.75rem;
        background: var(--bg);
      }}
      .view h2 {{
        margin: 0;
        text-transform: uppercase;
      }}
      .view p {{
        margin: 0;
        opacity: 0.92;
        line-height: 1.45;
      }}
      ul {{
        margin: 0;
        padding-left: 1rem;
      }}
      button {{
        border: 1px solid var(--line);
        background: transparent;
        color: inherit;
        border-radius: 8px;
        padding: 0.45rem 0.75rem;
        cursor: pointer;
      }}
      .nav {{
        display: flex;
        justify-content: space-between;
        margin-top: 1rem;
      }}
      .globe-view {{
        position: relative;
        min-height: 540px;
        border-radius: 10px;
        overflow: hidden;
        background: var(--bg);
      }}
      .globe-canvas {{
        position: absolute;
        inset: 0;
      }}
      .globe-tooltip {{
        position: absolute;
        display: none;
        pointer-events: none;
        font-size: 12px;
        background: rgba(245, 245, 245, 0.93);
        color: #101010;
        border: 1px solid rgba(16, 16, 16, 0.22);
        border-radius: 4px;
        padding: 6px 8px;
        z-index: 10;
      }}
      .globe-copy {{
        position: absolute;
        left: 20px;
        bottom: 20px;
        max-width: min(460px, 70%);
        padding: 0.7rem 0.85rem;
        background: rgba(245, 245, 245, 0.88);
        border: 1px solid rgba(16, 16, 16, 0.2);
        border-radius: 10px;
        backdrop-filter: blur(3px);
      }}
      .globe-copy h3 {{
        margin: 0 0 0.25rem 0;
        font-size: 0.88rem;
      }}
      .globe-copy p {{
        font-size: 0.76rem;
      }}
      .marker-grid {{
        display: grid;
        gap: 0.5rem;
        margin-top: 0.4rem;
        max-height: min(46vh, 420px);
        overflow: auto;
        padding-right: 0.2rem;
      }}
      .marker-row {{
        display: grid;
        grid-template-columns: minmax(220px, 1.2fr) minmax(90px, 0.45fr) 1fr;
        gap: 0.7rem;
        align-items: center;
        padding: 0.48rem 0.58rem;
        border: 1px solid var(--line);
        border-radius: 9px;
        background: rgba(255, 255, 255, 0.28);
      }}
      .marker-name {{
        font-size: 0.8rem;
        font-weight: 600;
      }}
      .marker-energy {{
        font-size: 0.78rem;
        opacity: 0.85;
      }}
      .marker-hash {{
        font-size: 0.92rem;
        letter-spacing: 0.04em;
        white-space: nowrap;
        overflow: hidden;
        color: #d65f00;
        text-shadow: 0 0 10px rgba(214, 95, 0, 0.2);
      }}
      ::view-transition-old(root),
      ::view-transition-new(root) {{
        mask-image: var(--mi);
        mask-repeat: no-repeat;
        mask-size: calc(var(--ms) + 1px) calc(var(--ms) + 1px);
        mask-position: var(--mp, -100% -100%);
      }}
      [data-transition='outIn']::view-transition-old(root) {{
        animation: maskOut var(--speed) ease forwards reverse;
      }}
      [data-transition='outIn']::view-transition-new(root) {{
        animation: maskIn var(--speed) calc(var(--speed) * 1.1) ease forwards;
      }}
    </style>
    <style id="vt"></style>
    <script src="https://unpkg.com/globe.gl"></script>
    <script src="https://unpkg.com/three"></script>
    <script src="https://unpkg.com/solar-calculator"></script>
  </head>
  <body>
    <div class="stage">
    <div class="shell">
      <div class="controls">
        <label>Cells <input id="cells" type="range" min="3" max="21" step="2" value="11" /></label>
        <label>Speed <input id="speed" type="range" min="0.2" max="1.2" step="0.01" value="0.36" /></label>
      </div>
      <div id="view" class="view"></div>
      <div class="nav">
        <button id="prev">Previous</button>
        <button id="next">Next</button>
      </div>
    </div>
    </div>

    <script>
      const views = {payload};
      const crackPoints = {points_payload};
      let index = 0;
      let globeRef = null;
      const view = document.getElementById("view");
      let frameTimer = null;
      let frameRaf = 0;

      const shuffleArray = (arr) => {{
        for (let i = arr.length - 1; i > 0; i--) {{
          const j = Math.floor(Math.random() * (i + 1));
          [arr[i], arr[j]] = [arr[j], arr[i]];
        }}
        return arr;
      }};

      const getPositions = (frame, positions, cells) => {{
        const slices = [];
        for (let i = 0; i < cells; i++) {{
          if (i < frame) slices.push(positions.slice(i * cells, (i + 1) * cells));
          else slices.push(positions.slice(frame * cells, (frame + 1) * cells));
        }}
        return slices.join(",");
      }};

      const getFrames = (positions, cells) => {{
        let frames = "";
        const shuffled = shuffleArray(positions.slice());
        for (let f = 1; f < cells; f++) {{
          const sineFrame = Math.floor(Math.sin((f / cells) * (Math.PI / 2)) * 100);
          frames += `${{sineFrame}}% {{ --mp: ${{getPositions(f, shuffled, cells)}}; }}`;
        }}
        frames += `100% {{ --mp: ${{positions.join(",")}}; }}`;
        return frames;
      }};

      const genStyles = () => {{
        const cells = Number(document.getElementById("cells").value);
        const positions = [];
        const mid = Math.ceil(cells * 0.5);
        for (let p = 0; p < Math.pow(cells, 2); p++) {{
          const x = p % cells;
          const y = Math.floor(p / cells);
          const xm = x + 1 - mid;
          const ym = y + 1 - mid;
          positions.push(`calc(50% + (var(--ms) * ${{xm}})) calc(50% + (var(--ms) * ${{ym}}))`);
        }}
        const maskIn = `@keyframes maskIn {{${{getFrames(positions, cells)}}}}`;
        const maskOut = `@keyframes maskOut {{${{getFrames(positions, cells)}}}}`;
        document.querySelector("#vt").innerHTML = `
          :root {{
            --mi: ${{new Array(Math.pow(cells, 2)).fill("linear-gradient(#fff 0 0)").join(",")}};
            --size: ${{Math.ceil(100 / cells)}};
          }}
          ${{maskIn}}
          ${{maskOut}}
        `;
      }};

      const postFrameHeight = () => {{
        const shell = document.querySelector(".shell");
        if (!shell || !window.parent) return;
        const height = Math.ceil(shell.getBoundingClientRect().height + 24);
        window.parent.postMessage({{ type: "streamlit:setFrameHeight", height }}, "*");
      }};

      const scheduleFrameHeight = (delay = 0) => {{
        if (frameTimer) clearTimeout(frameTimer);
        frameTimer = setTimeout(postFrameHeight, delay);
      }};

      const requestFrameHeight = () => {{
        if (frameRaf) cancelAnimationFrame(frameRaf);
        frameRaf = requestAnimationFrame(postFrameHeight);
      }};

      const attachAutoHeight = () => {{
        const shell = document.querySelector(".shell");
        if (!shell) return;
        if (typeof ResizeObserver !== "undefined") {{
          const ro = new ResizeObserver(() => requestFrameHeight());
          ro.observe(shell);
          ro.observe(document.body);
        }}
        if (typeof MutationObserver !== "undefined") {{
          const mo = new MutationObserver(() => requestFrameHeight());
          mo.observe(shell, {{ childList: true, subtree: true, characterData: true }});
        }}
        window.addEventListener("load", () => scheduleFrameHeight(30));
      }};

      const setupGlobeView = () => {{
        const container = document.getElementById("crack-globe-view");
        const tooltip = document.getElementById("crack-globe-tooltip");
        if (!container || !window.Globe) return;

        globeRef = Globe()(container)
          .globeImageUrl("https://unpkg.com/three-globe/example/img/earth-night.jpg")
          .backgroundColor(getComputedStyle(document.documentElement).getPropertyValue("--bg").trim() || "#f5f5f5")
          .heatmapPointLat("lat")
          .heatmapPointLng("lng")
          .heatmapPointWeight("energy")
          .heatmapBandwidth(1.85)
          .heatmapColorSaturation(1.7)
          .enablePointerInteraction(true)
          .pointsData(crackPoints)
          .pointLat((d) => d.lat)
          .pointLng((d) => d.lng)
          .pointAltitude((d) => d.energy * 0.001)
          .pointColor(() => "rgba(255,150,0,0.95)")
          .pointRadius(0.48)
          .onPointHover((d) => {{
            if (!tooltip) return;
            if (d) {{
              tooltip.style.display = "block";
              tooltip.innerHTML = `<b>${{d.name}}</b><br/>Energy: ${{d.energy}}`;
            }} else {{
              tooltip.style.display = "none";
            }}
          }});

        globeRef.heatmapsData([crackPoints]);
        globeRef.controls().autoRotate = true;
        globeRef.controls().autoRotateSpeed = 2.2;

        const resizeGlobe = () => {{
          if (!globeRef) return;
          globeRef.width(container.clientWidth);
          globeRef.height(container.clientHeight);
        }};
        resizeGlobe();
        scheduleFrameHeight(40);
        window.addEventListener("resize", resizeGlobe);
        container.addEventListener("mousemove", (e) => {{
          if (!tooltip || tooltip.style.display === "none") return;
          const rect = container.getBoundingClientRect();
          tooltip.style.left = `${{e.clientX - rect.left + 12}}px`;
          tooltip.style.top = `${{e.clientY - rect.top + 12}}px`;
        }});
      }};

      const render = () => {{
        const item = views[index] || {{}};
        const bullets = (item.bullets || []).map((b) => `<li>${{b}}</li>`).join("");
        if (item.kind === "markers") {{
          const sorted = crackPoints
            .slice()
            .sort((a, b) => Number(b.energy || 0) - Number(a.energy || 0));
          const rows = sorted.map((point) => {{
            const energy = Number(point.energy || 0);
            const level = Math.max(3, Math.min(14, Math.round(energy / 7)));
            const hashes = "#".repeat(level);
            return `
              <div class="marker-row">
                <div class="marker-name">${{point.name}}</div>
                <div class="marker-energy">${{energy.toFixed(0)}}</div>
                <div class="marker-hash">${{hashes}}</div>
              </div>
            `;
          }}).join("");
          view.innerHTML = `
            <h2>${{item.title || ""}}</h2>
            <p>${{item.subtitle || ""}}</p>
            <p>${{item.body || ""}}</p>
            <div class="marker-grid">${{rows}}</div>
            <ul>${{bullets}}</ul>
            <p>Step ${{index + 1}} / ${{views.length}}</p>
          `;
          scheduleFrameHeight(30);
          return;
        }}
        if (item.kind === "globe") {{
          view.innerHTML = `
            <h2>${{item.title || ""}}</h2>
            <p>${{item.subtitle || ""}}</p>
            <div class="globe-view">
              <div id="crack-globe-view" class="globe-canvas"></div>
              <div id="crack-globe-tooltip" class="globe-tooltip"></div>
              <div class="globe-copy">
                <h3>Cryosphere crack map</h3>
                <p>${{item.body || ""}}</p>
              </div>
            </div>
            <ul>${{bullets}}</ul>
            <p>Step ${{index + 1}} / ${{views.length}}</p>
          `;
          setupGlobeView();
          scheduleFrameHeight(80);
          return;
        }}
        view.innerHTML = `
          <h2>${{item.title || ""}}</h2>
          <p>${{item.subtitle || ""}}</p>
          <p>${{item.body || ""}}</p>
          <ul>${{bullets}}</ul>
          <p>Step ${{index + 1}} / ${{views.length}}</p>
        `;
        scheduleFrameHeight(30);
      }};

      const transitionTo = (nextIndex) => {{
        const apply = () => {{
          index = (nextIndex + views.length) % views.length;
          render();
          scheduleFrameHeight(70);
        }};
        if (!document.startViewTransition) {{
          apply();
          return;
        }}
        document.startViewTransition(apply).finished.then(() => scheduleFrameHeight(100));
      }};

      document.getElementById("prev").addEventListener("click", () => transitionTo(index - 1));
      document.getElementById("next").addEventListener("click", () => transitionTo(index + 1));
      document.getElementById("cells").addEventListener("input", () => {{
        genStyles();
        scheduleFrameHeight(30);
      }});
      document.getElementById("speed").addEventListener("input", (e) => {{
        document.documentElement.style.setProperty("--duration", e.target.value);
        scheduleFrameHeight(30);
      }});
      window.addEventListener("resize", () => scheduleFrameHeight(70));

      attachAutoHeight();
      genStyles();
      render();
      scheduleFrameHeight(140);
    </script>
  </body>
</html>
"""
