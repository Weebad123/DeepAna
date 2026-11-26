// Drives the DeepAna dashboard in headless Chromium over the DevTools
// protocol and checks click-through navigation end to end:
//
//   links in answers -> detail panel in place of the results -> links inside
//   the panel -> browser Back / Forward -> reload -> a pasted URL -> the
//   panel's own Back -> loading, not found and bad-link states,
//
// counting the detail requests the page makes (one per navigation, none for
// a view it already has, no polling).
//
// Usage: node navigate.mjs <base-url> <chromium> <profile-dir> <fixtures.json>
// fixtures: { "mint", "account", "signature", "failed_signature", "unknown_signature", "slot" }
// Prints one line per check; exits non-zero if any fails.

import { spawn } from "node:child_process";

const [base, chromiumPath, profileDir, fixturesJson] = process.argv.slice(2);
const F = JSON.parse(fixturesJson);
const port = 9300 + Math.floor(Math.random() * 600);
const chrome = spawn(chromiumPath, [
  "--headless=new", "--no-sandbox", "--disable-gpu", "--no-first-run", `--remote-debugging-port=${port}`,
  "--window-size=1500,1000", `--user-data-dir=${profileDir}`, "about:blank",
], { stdio: "ignore" });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ws, nextId = 0;
const pending = new Map();
const listeners = [];

async function connect() {
  for (let i = 0; i < 100; i++) {
    try {
      const tabs = await (await fetch(`http://127.0.0.1:${port}/json`)).json();
      const page = tabs.find((t) => t.type === "page");
      ws = new WebSocket(page.webSocketDebuggerUrl);
      await new Promise((res, rej) => { ws.onopen = res; ws.onerror = rej; });
      ws.onmessage = (m) => {
        const d = JSON.parse(m.data);
        if (d.id && pending.has(d.id)) { pending.get(d.id)(d); pending.delete(d.id); }
        else if (d.method) for (const l of listeners) l(d);
      };
      return;
    } catch (_) { await sleep(150); }
  }
  throw new Error("chromium did not start");
}
const send = (method, params = {}) => new Promise((res, rej) => {
  const id = ++nextId;
  pending.set(id, (d) => d.error ? rej(new Error(`${method}: ${d.error.message}`)) : res(d.result));
  ws.send(JSON.stringify({ id, method, params }));
});
async function js(expr) {
  const r = await send("Runtime.evaluate", { expression: expr, awaitPromise: true, returnByValue: true });
  if (r.exceptionDetails) throw new Error(`${expr.slice(0, 80)}: ${r.exceptionDetails.exception?.description || r.exceptionDetails.text}`);
  return r.result.value;
}
async function until(expr, what, ms = 15000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (await js(expr).catch(() => false)) return; await sleep(50); }
  throw new Error(`timed out waiting for ${what}; panel: ${await js(`(document.getElementById("detail-body")||{}).innerText?.slice(0,300)`)}`);
}

// Detail requests: what a navigation costs.
const detailRequests = [];
listeners.push((d) => {
  if (d.method === "Network.requestWillBeSent") {
    const u = d.params.request.url;
    if (u.includes("/api/oracle/profile/") || u.includes("/api/oracle/drill")) detailRequests.push(u.slice(base.length));
  }
});
const errors = [];
listeners.push((d) => {
  if (d.method === "Runtime.exceptionThrown") errors.push(d.params.exceptionDetails.exception?.description || d.params.exceptionDetails.text);
});

const results = [];
const check = (cond, what) => { results.push(`${cond ? "ok  " : "FAIL"} ${what}`); if (!cond) process.exitCode = 1; };
const hash = () => js("location.hash");
const panelOpen = () => js(`!document.getElementById("detail").hidden && document.getElementById("answer").hidden`);
const heading = () => js(`document.querySelector("#detail-body h3")?.innerText ?? ""`);
const rendered = (prefix) => `!document.querySelector("#detail-body .loading") && (document.querySelector("#detail-body h3")?.innerText ?? "").startsWith(${JSON.stringify(prefix)})`;
async function counted(fn) { const before = detailRequests.length; await fn(); await sleep(150); return detailRequests.slice(before); }
const clickLink = (selector) => js(`(() => { const a = document.querySelector(${JSON.stringify(selector)}); if (!a) throw new Error("no link " + ${JSON.stringify(selector)}); a.click(); return a.getAttribute("href"); })()`);

try {
  await connect();
  await send("Page.enable"); await send("Runtime.enable"); await send("Network.enable");
  await send("Page.navigate", { url: base + "/" });
  await until(`document.getElementById("conn")?.textContent === "live"`, "the stream");

  // ---- 1. An answer: every mint is a link, every row has "details ›".
  await js(`(() => { const i = document.getElementById("ask-input"); i.value = "top 10 mints by transfers in the last hour"; document.getElementById("ask-form").requestSubmit(); })()`);
  await until(`document.querySelectorAll("#answer tbody tr").length > 0`, "the answer");
  const rows = await js(`document.querySelectorAll("#answer tbody tr").length`);
  const mintLinks = await js(`[...document.querySelectorAll("#answer a.lnk[data-entity=mint]")].map((a) => a.getAttribute("href"))`);
  const details = await js(`document.querySelectorAll("#answer a.lnk[data-entity=rows]").length`);
  check(rows === 3 && mintLinks.length === 3 && details === 3, `answer: ${rows} rows, ${mintLinks.length} mint links, ${details} details links`);
  check(mintLinks.every((h) => /^#\/mint\/[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(h)), "every mint link is #/mint/<address>");
  check(await js(`document.querySelectorAll("#answer a.lnk[data-entity=mint]")[0].textContent`) === mintLinks[0].slice(7), "a mint link's text is the mint");

  // ---- 2. Click a mint: the panel replaces the results, one request.
  let reqs = await counted(async () => {
    await clickLink("#answer a.lnk[data-entity=mint]");
    await until(rendered("mint "), "the mint profile");
  });
  check(await hash() === mintLinks[0], `URL is ${mintLinks[0].slice(0, 20)}…`);
  check(await panelOpen(), "the panel replaces the results");
  check(await js(`getComputedStyle(document.getElementById("detail-back")).display !== "none" && document.getElementById("detail-back").offsetWidth > 0`), "Back is visible");
  check(reqs.length === 1 && reqs[0] === `/api/oracle/profile/mint/${mintLinks[0].slice(7)}`, `one request: ${reqs.join(", ")}`);
  check(await js(`document.querySelectorAll("#detail-body dl").length > 0 && document.querySelectorAll("#detail-body table").length > 0`), "the profile renders as key-value blocks and tables");
  const profileLinks = await js(`[...document.querySelectorAll("#detail-body a.lnk")].map((a) => a.dataset.entity)`);
  check(profileLinks.includes("token_account") && profileLinks.includes("slot") && profileLinks.includes("rows"),
    `the profile links its token accounts, slots and rows: ${[...new Set(profileLinks)].join(",")}`);

  // ---- 3. Inside the panel: the mint's transfers, then a transaction.
  reqs = await counted(async () => {
    await clickLink("#detail-body a.lnk[data-entity=rows]");
    await until(rendered("Rows behind"), "the transfers");
  });
  check(reqs.length === 1 && reqs[0] === "/api/oracle/drill", `rows: one request (${reqs.join(", ")})`);
  const txHref = await js(`document.querySelector("#detail-body a.lnk[data-entity=tx_signature]").getAttribute("href")`);
  check(/^#\/tx\/[1-9A-HJ-NP-Za-km-z]{64,88}\?slot=\d+&ix=1$/.test(txHref), `a transfer's signature links with its slot and instruction: ${txHref.slice(0, 16)}…${txHref.slice(-16)}`);
  reqs = await counted(async () => {
    await clickLink("#detail-body a.lnk[data-entity=tx_signature]");
    await until(rendered("Transaction"), "the transaction");
  });
  check(reqs.length === 1 && reqs[0].startsWith("/api/oracle/profile/transaction/"), "transaction: one request");
  const tx = await js(`(() => { const b = document.getElementById("detail-body"); return {
    text: b.innerText, sections: [...b.querySelectorAll("h4")].map((h) => h.innerText.split(" (")[0].toLowerCase()),
    focus: b.querySelector(".ix.focus")?.dataset.path, status: b.querySelector(".tag")?.innerText } })()`);
  check(tx.text.includes(txHref.slice(5, txHref.indexOf("?"))), "the header names the signature");
  check(["memo", "token transfers", "instructions", "accounts", "logs"].every((s) => tx.sections.includes(s)), `sections: ${tx.sections.join(", ")}`);
  check(/slot\s+[\d,]+ · index 0/.test(tx.text) && /fee\s+5,000 lamports/.test(tx.text) && /compute units\s+1,000/.test(tx.text), "summary: slot, fee, compute units");
  check(tx.focus === "1", "the drilled instruction (#1) is focused");
  check(/SetComputeUnitLimit/.test(tx.text), "the budget instruction is decoded");
  // Every link in the transaction panel names the right thing.
  const panelLinks = await js(`[...document.querySelectorAll("#detail-body a.lnk")].map((a) => ({ h: a.getAttribute("href"), e: a.dataset.entity, t: a.textContent }))`);
  const shape = { slot: /^#\/slot\/\d+$/, mint: /^#\/mint\//, program: /^#\/program\//, token_account: /^#\/account\// };
  check(panelLinks.length >= 5 && panelLinks.every((l) => shape[l.e]?.test(l.h)), `${panelLinks.length} panel links, each to its kind: ${[...new Set(panelLinks.map((l) => l.e))].join(",")}`);

  // ---- 4. Every link in the transaction panel lands on its data.
  for (const l of panelLinks.filter((l, i, all) => all.findIndex((x) => x.h === l.h) === i)) {
    await js(`location.hash = ${JSON.stringify(l.h)}`);
    const want = { slot: "Slot", mint: "mint", program: "program", token_account: "token account" }[l.e];
    await until(rendered(want), `${l.h}`);
    const h = await heading();
    const id = l.h.split("/")[2];
    const ok = l.e === "slot" ? h.replace(/,/g, "") === `Slot ${id}` : h.endsWith(id);
    check(ok, `${l.e} link ${l.h.slice(0, 24)}… lands on "${h.slice(0, 40)}${h.length > 40 ? "…" : ""}"`);
    await js("history.back()");
    await until(rendered("Transaction"), "back to the transaction");
  }

  // ---- 5. Browser Back and Forward redraw from memory: no request.
  reqs = await counted(async () => {
    await js("history.back()");
    await until(rendered("Rows behind"), "back to the rows");
    await js("history.back()");
    await until(rendered("mint "), "back to the mint");
    await js("history.forward()");
    await until(rendered("Rows behind"), "forward to the rows");
    await js("history.forward()");
    await until(rendered("Transaction"), "forward to the transaction");
  });
  check(reqs.length === 0, `Back, Back, Forward, Forward: ${reqs.length} requests (cached)`);
  check(await hash() === txHref, "Forward returns to the same URL");

  // ---- 6. Back from the first view returns to the results, still there.
  await js("history.back()"); await until(rendered("Rows behind"), "rows");
  await js("history.back()"); await until(rendered("mint "), "mint");
  await js("history.back()");
  await until(`document.getElementById("detail").hidden`, "the results");
  check(await hash() === "" && await js(`document.querySelectorAll("#answer a.lnk[data-entity=mint]").length`) === 3, "Back past the first view shows the answer again");

  // ---- 7. Reload reproduces the view: one request.
  await js(`location.hash = ${JSON.stringify(txHref)}`);
  await until(rendered("Transaction"), "the transaction again");
  reqs = await counted(async () => {
    await send("Page.reload");
    await until(rendered("Transaction"), "the transaction after reload");
  });
  const after = reqs.filter((u) => u.includes("/profile/"));
  check(after.length === 1 && after[0] === `/api/oracle/profile/transaction/${txHref.slice(5, txHref.indexOf("&"))}`, `reload: one request, the same view (${after.join(", ")})`);
  check(await js(`document.querySelector("#detail-body .ix.focus")?.dataset.path`) === "1", "reload keeps the focused instruction");

  // ---- 8. A pasted URL: a fresh tab state; the panel's Back goes to the results.
  // about:blank first: navigating within the page would only change its hash.
  await send("Page.navigate", { url: "about:blank" });
  await until(`location.href === "about:blank"`, "a blank tab");
  await send("Page.navigate", { url: `${base}/#/account/${F.account}` });
  await until(rendered("token account"), "the pasted token account");
  check((await heading()).endsWith(F.account), "a pasted #/account/<id> shows that token account");
  check(await js(`document.querySelectorAll("#detail-body table").length`) > 0, "its top counterparties are a table");
  await js(`document.getElementById("detail-back").click()`);
  await until(`document.getElementById("detail").hidden && !document.getElementById("answer").hidden`, "the results after Back");
  check(await hash() === "", "the panel's Back on a pasted view clears the URL");
  await js("history.back()");
  await until(rendered("token account"), "browser Back returns to the pasted view");
  check(true, "browser Back after the panel's Back returns to the pasted view");

  // ---- 9. The panel's Back after in-page navigation is the browser's Back.
  await clickLink("#detail-body a.lnk[data-entity=token_account]");
  await until(`location.hash.startsWith("#/account/") && ${rendered("token account")}`, "a counterparty");
  const counterparty = await hash();
  await js(`document.getElementById("detail-back").click()`);
  await until(`location.hash === ${JSON.stringify(`#/account/${F.account}`)} && ${rendered("token account")}`, "Back to the first account");
  check(counterparty !== `#/account/${F.account}`, "the panel's Back steps back one view");

  // ---- 10. States: loading (response held), not found, bad link, failure with retry.
  await send("Fetch.enable", { patterns: [{ urlPattern: `*/api/oracle/profile/slot/${F.slot}*` }] });
  let paused = null;
  const onPause = (d) => { if (d.method === "Fetch.requestPaused") paused = d.params.requestId; };
  listeners.push(onPause);
  await js(`location.hash = "#/slot/${F.slot}"`);
  await until(`!!document.querySelector("#detail-body .loading")`, "the loading state");
  check((await js(`document.querySelector("#detail-body .loading").innerText`)).startsWith("loading slot"), "loading state while the request is held");
  for (let i = 0; i < 100 && !paused; i++) await sleep(50);
  await send("Fetch.continueRequest", { requestId: paused });
  await until(rendered("Slot"), "the slot after release");
  check(true, "the view renders once the response arrives");
  await send("Fetch.disable");
  listeners.splice(listeners.indexOf(onPause), 1);

  await js(`location.hash = "#/tx/${F.unknown_signature}"`);
  await until(`!!document.querySelector('#detail-body [data-state="not-found"]')`, "not found");
  check(/Not found in slots/.test(await js(`document.getElementById("detail-body").innerText`)), "found=false: says where it looked");
  await js(`location.hash = "#/tx/${F.failed_signature}"`);
  await until(`!!document.querySelector("#detail-body .failure")`, "the failed transaction");
  check(/Failed in SPL Token at instruction #1: custom program error: 0x1/.test(await js(`document.querySelector("#detail-body .failure").innerText`)), "a failed transaction says where and why");
  await js(`location.hash = "#/tx/not-a-signature"`);
  await until(`/does not name anything/.test(document.getElementById("detail-body").innerText)`, "the bad link state");
  check(true, "a bad link is a notice, not an error");
  await send("Fetch.enable", { patterns: [{ urlPattern: "*/api/oracle/profile/program/*" }] });
  const onFail = (d) => { if (d.method === "Fetch.requestPaused") send("Fetch.failRequest", { requestId: d.params.requestId, errorReason: "ConnectionRefused" }); };
  listeners.push(onFail);
  await js(`location.hash = "#/program/TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"`);
  await until(`!!document.querySelector("#detail-body [data-retry]")`, "the failure state");
  check(/Could not load/.test(await js(`document.getElementById("detail-body").innerText`)), "a failed request shows the error and a Retry");
  listeners.splice(listeners.indexOf(onFail), 1);
  await send("Fetch.disable");
  await js(`document.querySelector("#detail-body [data-retry]").click()`);
  await until(rendered("program "), "the program after Retry");
  check(true, "Retry loads it");

  // ---- 11. No polling: an open view makes no further detail requests.
  reqs = await counted(() => sleep(3000));
  check(reqs.length === 0, `3 s on an open view: ${reqs.length} detail requests`);

  // ---- 12. The cache keeps the last 20 views and no more.
  const cache = await js(`(() => { for (let i = 0; i < 30; i++) cachePut("k" + i, { found: true }); return [detailCache.size, detailCache.has("k9"), detailCache.has("k10")]; })()`);
  check(cache[0] === 20 && !cache[1] && cache[2], `cache holds the last ${cache[0]} entries`);

  check(errors.length === 0, `no uncaught page errors${errors.length ? ": " + errors.join(" | ") : ""}`);
} catch (err) {
  results.push(`FAIL ${err.message}`);
  process.exitCode = 1;
}
console.log(results.join("\n"));
chrome.kill("SIGKILL");
process.exit(process.exitCode || 0);
