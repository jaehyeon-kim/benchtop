# Next.js dashboard

The Next.js version of the live-dashboard, a TypeScript and React page that shows the WebSocket server's feed as three numbers and two charts. The project's [README](../README.md) explains the whole system and how to start the services and the server first.

## Run it

With the simulation and the WebSocket server running:

```bash
pnpm install
pnpm dev
```

Open http://127.0.0.1:3000 and tick **Connect to WS Server**. `pnpm dev` restarts the page whenever a source file changes.

## Files

- `src/app/page.tsx`: the page, with the checkbox, the three cards and the two charts.
- `src/lib/useDashboard.ts`: follows the WebSocket at `ws://127.0.0.1:8000/ws` and turns each message into cards and charts.
- `src/lib/processing.ts`: the calculations, the same as `sales/dashboard/metrics.py` in the Streamlit version.
- `src/components/metric.tsx`: one card, with its change since the last message.
- `src/app/layout.tsx` and `src/app/providers.tsx`: the page's frame, and the NextUI provider its components need.

[Concepts](../docs/concepts.md#react-state-and-effects) explains the hooks and why the page is a client component.

## Checks

```bash
pnpm test                # the Vitest tests of processing.ts
pnpm exec tsc --noEmit   # the TypeScript type check
```
