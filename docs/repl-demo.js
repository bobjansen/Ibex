(() => {
  "use strict";

  const transcript = [
    {
      command: "import data_gen;",
      output: "time: 798 us",
      elapsed: 1,
    },
    {
      command: 'let ticks = gen_ticks(10000000, "AAPL,MSFT,NVDA");',
      output: "time: 580.712 ms",
      elapsed: 581,
    },
    {
      command: "ticks[select { avg_price = mean(price), traded = sum(volume) }, by symbol, order { avg_price desc }];",
      output: `rows: 3
+--------+-----------+-------------+
| symbol | avg_price | traded      |
+--------+-----------+-------------+
| "AAPL" | 279.846   | 16671488213 |
| "NVDA" | 279.7652  | 16669109752 |
| "MSFT" | 279.743   | 16662431719 |
+--------+-----------+-------------+
time: 100.744 ms`,
      elapsed: 101,
    },
  ];

  const delay = (milliseconds) => new Promise((resolve) => window.setTimeout(resolve, milliseconds));
  const typeCommand = (line, command, isCurrent) => new Promise((resolve) => {
    const charactersPerSecond = 100;
    let startedAt;

    const frame = (now) => {
      if (!isCurrent()) {
        resolve();
        return;
      }
      startedAt ??= now;
      const count = Math.min(command.length, Math.floor(((now - startedAt) / 1000) * charactersPerSecond));
      line.textContent = `ibex> ${command.slice(0, count)}`;
      if (count < command.length) {
        window.requestAnimationFrame(frame);
      } else {
        resolve();
      }
    };

    window.requestAnimationFrame(frame);
  });

  document.querySelectorAll("[data-repl-demo]").forEach((terminal) => {
    const output = terminal.querySelector("[data-repl-output]");
    const replay = terminal.querySelector("[data-repl-replay]");
    let started = false;
    let run = 0;

    const addLine = (text, className = "") => {
      const line = document.createElement("span");
      line.className = `terminal-line ${className}`;
      line.textContent = text;
      output.append(line);
      terminal.scrollTop = terminal.scrollHeight;
      return line;
    };

    const play = async () => {
      const currentRun = ++run;
      output.replaceChildren();
      replay.disabled = true;
      replay.textContent = "Running";

      for (const { command, output: response, elapsed } of transcript) {
        const line = addLine("ibex> ", "terminal-command terminal-typing");
        await typeCommand(line, command, () => currentRun === run);
        if (currentRun !== run) return;
        line.classList.remove("terminal-typing");
        line.classList.add("terminal-waiting");
        await delay(elapsed);
        line.classList.remove("terminal-waiting");
        response.split("\n").forEach((responseLine) => addLine(responseLine, "terminal-response"));
      }

      if (currentRun === run) {
        replay.disabled = false;
        replay.textContent = "Replay";
      }
    };

    replay.addEventListener("click", play);
    const begin = () => {
      if (!started) {
        started = true;
        play();
      }
    };
    if (!("IntersectionObserver" in window)) {
      begin();
      return;
    }
    const observer = new IntersectionObserver((entries) => {
      if (!started && entries.some((entry) => entry.isIntersecting)) {
        begin();
        observer.disconnect();
      }
    }, { threshold: 0.28 });
    observer.observe(terminal);
  });
})();
