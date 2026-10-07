# Concepts behind the design

The ideas the [live-dashboard](../README.md) project is built on, each explained briefly and tied to where the code uses it.

## Pushing data compared with polling it

A dashboard can get new data in two ways:

- **Polling:** the dashboard asks the server at a fixed interval, and the server answers each time, whether or not anything changed.
- **Pushing:** the dashboard opens one connection and waits, and the server sends data when it has some.

Polling works with any web server, but every browser runs its own timer and sends a full request each time. Pushing needs a connection that stays open, and the dashboard does nothing between messages. Here the server pushes. It still reads PostgreSQL on a timer, but that timer is in one place, the server, rather than in every browser.

## WebSockets compared with HTTP requests

An HTTP request is one question and one answer, so the server cannot send anything the browser has not asked for. A **WebSocket** starts as an HTTP request and then stays open. The browser sends the headers `Upgrade: websocket` and `Connection: Upgrade`. The server answers with status `101 Switching Protocols`. From then on the connection carries messages both ways until either side closes it.

Here [`server.py`](../sales/api/server.py) declares the endpoint with FastAPI's `@app.websocket("/ws")`. For each connection it calls `accept`, then loops: read the records, send them with `send_json` as one text message of JSON, and sleep. The loop ends when the client leaves, and the server closes its PostgreSQL connection. Each dashboard holds its own connection, and the server keeps no list of clients.

## Lookback window and refresh interval

Two settings in [`config.py`](../sales/core/config.py) decide what the dashboards see: `LOOKBACK_MINUTES = 5`, the window of order items in each message, and `REFRESH_SECONDS = 5`, how often each connection sends one.

Each message holds the whole window again, not only what changed. So the dashboards recalculate everything from each message, and one that connects late is correct from its first message. The cost grows with the number of connections, because each runs its own query: ten open dashboards run it ten times every five seconds. It also grows with a longer window, which sends more rows, and a shorter interval, which queries more often for data that is only a little newer.

The query, `RECENT_ITEMS` in [`postgres.py`](../sales/stores/postgres.py), filters with `clock_timestamp()`. The tables store times as ISO 8601 text, so the query casts them with `::timestamptz`. PostgreSQL's `current_timestamp` returns the start time of the current transaction, while `clock_timestamp()` returns the actual current time.

## Streamlit's rerun model

A Streamlit app is a Python script that Streamlit runs from top to bottom, adding an element to the page for each `st.*` call. When a user changes a widget, such as a checkbox, Streamlit runs the whole script again with the widget's new value. This is a **rerun**.

A rerun suits a page that changes when the user acts. A live feed must change while the user does nothing, so [`streamlit_app.py`](../sales/dashboard/streamlit_app.py) keeps its script running:

1. `st.empty()` reserves two slots, for the cards and the charts. Writing to a slot again replaces what it holds.
2. When **Connect to WS Server** is ticked, the script calls `asyncio.run(_follow(...))`, which connects with aiohttp and loops over the messages.
3. For each message it recalculates the numbers and charts, and writes them into the two slots.

Unticking the box asks for a rerun. Streamlit checks for that request each time the script sends an element to the page, so the loop stops at its next redraw, and the new run draws the page with zeros.

## React state and effects

React calls a component again, a **re-render**, when its data changes. React's **hooks** are functions, with names that start with `use`, that a component calls to use React's features. `useDashboard`, in [`useDashboard.ts`](../nextjs/src/lib/useDashboard.ts), is this project's own hook, built from three of React's:

- **`useState`** adds a state variable. Calling its set function stores a new value and triggers a re-render. The cards and the chart options are kept this way.
- **`useRef`** holds a value that is not needed for rendering, and changing it triggers no re-render. The last message's numbers are kept there, to work out each card's change.
- **`useEffect`** synchronises a component with an external system. React runs it after the first render, and again after a render in which one of its dependencies changed. The effect depends on `lastJsonMessage`, so it runs once per message.

The WebSocket comes from `react-use-websocket`. `useWebSocket(url, options, connect)` returns the latest message, parsed as JSON, as `lastJsonMessage`. The checkbox's value is passed as `connect`, and `false` closes the connection. `shouldReconnect: () => true` reconnects if the server goes away, and `share: false` gives each hook its own connection.

Next.js renders components on the server by default, as **server components**. A file that starts with `"use client"` makes its components, and the components it imports, **client components**, which also run in the browser. A component needs this for state, events or browser features. [`page.tsx`](../nextjs/src/app/page.tsx) has it, because it holds the checkbox's state and the WebSocket. [`providers.tsx`](../nextjs/src/app/providers.tsx) has it because [`layout.tsx`](../nextjs/src/app/layout.tsx), a server component, renders it, and NextUI's provider is itself a client component.

## ECharts options

ECharts draws a chart from one object of **options**. Streamlit passes it to `st_echarts(options=...)`, and Next.js to `<ReactECharts option={...}>`, which hands it to ECharts' `setOption`. The two dashboards build the same options, in [`metrics.py`](../sales/dashboard/metrics.py) and [`processing.ts`](../nextjs/src/lib/processing.ts): a category `xAxis` with its labels rotated 75 degrees, a value `yAxis`, one bar series with `colorBy: "data"` for a colour per bar, and a `grid`.

A **grid** is the rectangle the axes are drawn in. By default, `grid.left`, `grid.right`, `grid.top` and `grid.bottom` place the axes themselves, and the labels hang outside them. In ECharts 5.6.0 the default bottom is 70 pixels, which is too small for a rotated label such as "United Kingdom", so without `containLabel` the label is cut off. With `grid.containLabel: true`, those settings place the rectangle that holds the axes and their labels, so the labels always fit. The ECharts documentation recommends it when the length of the labels is hard to predict. streamlit-echarts bundles ECharts 6.1.0, which marks `containLabel` deprecated in favour of `grid.outerBoundsMode` but still honours it.

## Discrete-event simulation

A **discrete-event simulation** models a system as events at points in time: a visitor arrives, an order ships. Nothing changes between two events, so the model jumps from one to the next. dynamic-des builds such models on SimPy, and adds live parameters and connectors to databases and message queues.

The parts of the shop's model, in [`run.py`](../sales/simulation/run.py):

- **Processes:** a process is a Python generator that waits with `yield`. `visit` is one visitor, and `fulfil` is one order. `ctx.spawn` starts one, so many run at once.
- **Arrivals:** `add_arrival("visitor", dist="exponential", rate=4.0)` sets the gaps between visitors. An exponential gap with rate 4 has a mean of a quarter of a second, so four visitors arrive a second on average, at random moments.
- **Service times:** `add_service` sets how long a step takes. The shop's are lognormal, positive times with a long tail, given by a mean and standard deviation: a page view takes 4 seconds on average.
- **Resources:** `add_resource("pickers", current_cap=10, max_cap=20)` is a pool of 10 pickers. An order asks for one with `request()`, and waits in a queue if all are busy.
- **Patience:** `fulfil` waits for whichever comes first, a picker or a patience time of about two minutes (`picker | _wait(app, "patience")`). If patience wins, the order leaves the queue and is cancelled. Leaving a queue after waiting too long is called **reneging**.
- **Chances:** `add_variable` stores a probability, such as the 30% chance that a visitor buys.
- **Time:** `factor=1.0` runs one simulated second per second. The tests run the same model with `factor = 0.0`, as fast as possible.

An order moves through its statuses like this:

1. `Processing` while it waits for a picker, and while the picker packs it, for about 5 seconds.
2. `Shipped` once it is packed, for about 60 seconds in transit.
3. `Complete` when it reaches the customer.
4. `Returned` about 60 seconds later, for 10% of orders.

An order that finds no free picker within its patience time, about two minutes, becomes `Cancelled` instead.

An **egress** is a dynamic-des connector that sends the simulation's rows to another system. Each row goes to PostgreSQL through a `PostgresEgress` for its table. Orders and order items are upserted on `id`, so a status change updates the row. The other tables skip a row whose key exists, so a restarted simulation writes the products again without an error.

## Live changes through the parameter table

Every rate, time and chance lives in dynamic-des's **registry**, a store of named parameters such as `sales.arrival.visitor.rate`. The processes read the current value each time they draw one, so a change takes effect from the next draw.

Changes reach the registry through `PostgresIngress`, an **ingress**: a connector that brings changes into the simulation. It reads `dashboard.params`:

1. `sales.simulation.control` inserts a row with the path and the new value, marked not applied.
2. Every two seconds, the ingress sends each row not yet applied to the registry, and marks it applied.
3. When the simulation starts, the ingress first loads the latest applied value of each path, so a change survives a restart until the clean-up drops the table.

Raising the pickers' `current_cap` lets waiting orders take the new pickers straight away. Lowering it takes pickers away as they finish their current order, so no order being packed is interrupted.
