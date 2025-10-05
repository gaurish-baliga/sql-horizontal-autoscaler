# SQL Horizontal Autoscaler

Ever wondered what happens when your app goes viral and your database can't keep up? This project is a hands-on exploration of that very problem. It's a fully functional, proxy-based horizontal autoscaler for SQL databases, written in Go.

It automatically distributes queries, monitors load in real-time, and—most importantly—provisions new database shards on the fly when things get busy.

---

## 🚀 The Blueprint: Our Architecture

At its heart, the system separates the flow of data (the **Data Plane**) from the management and decision-making (the **Control Plane**).

<img width="719" height="459" alt="image" src="https://github.com/user-attachments/assets/2f53a021-b832-4045-a8bf-81fa34ca3142" />


This architecture is brought to life by three core components:

1.  🧠 **The Coordinator Service (The Brain):** This is the master of ceremonies. It runs in the background, constantly monitoring the real-time CPU, memory, and query load on every database shard. When it sees a server is overloaded, it makes the decision to scale.

2.  👮 **The Query Router (The Traffic Cop):** This is the single entry point for our application. Instead of talking to a database directly, our app sends its SQL queries here. The router intelligently parses the query to figure out which shard the data lives on and routes the request accordingly.

3.  🌐 **The Shard Cluster:** This is a group of standard, independent SQL database instances. Thanks to the Router and Coordinator, they act as one massive, cohesive database.

---

## 💡 The Details: LLD & Design Decisions

So, how does the magic actually happen? Here are a few key design choices that make the system work.

### 1. The "Smart" Router

A core decision was to make the router "smart." Instead of forcing the client application to know which shard to talk to, the router figures it out automatically.

- **How it works:** When a query like `SELECT * FROM users WHERE user_id = 123` arrives, a Go-based SQL parser (`xwb1989/sqlparser`) instantly analyzes the `WHERE` clause. It finds the shard key (`user_id`) and its value (`123`).
- **What if there's no key?** If the query is something like `SELECT COUNT(*) FROM users`, the router performs a **scatter-gather**: it concurrently sends the query to *all* shards and merges the results.
- **Why this way?** This makes the developer experience incredibly simple. The application code just writes standard SQL and remains completely unaware of the complex sharded architecture underneath.

### 2. Dynamic Provisioning with Docker

The "auto" in autoscaler is the most exciting part. When the Coordinator decides to scale, it doesn't just send an alert—it takes action.

- **How it works:** The Coordinator uses Go to execute `docker` commands directly. It spins up a brand-new MySQL container, configures it with a new database and user, waits for it to be healthy, and then seamlessly integrates it into the cluster's consistent hashing ring.
- **Why this way?** This creates a truly self-contained and automated scaling experience. The system doesn't just scale logically; it scales its own physical infrastructure.

### 3. Real-Time Metrics for Real Decisions

Scaling decisions are only as good as the data they're based on.

- **How it works:** The Coordinator doesn't use dummy data. It uses the `gopsutil` library to collect the *actual* CPU and memory usage from the host system where the Docker containers are running. It also connects to each shard to get real-time database stats like active connections and row counts.
- **Why this way?** This ensures that scaling decisions are based on real-world performance, making the autoscaler genuinely responsive to actual load.

---

## 🧪 See It In Action: Testing the System

The best part of this project is watching it work. The included test script (`test.sh`) is a short movie of the system's life.

### Quick Start

Just three commands are all you need to see the entire scaling process from start to finish.

```bash
# 1. Set up the initial environment (starts 1 Docker-based shard)
./setup.sh

# 2. Build the Go application
go build -o sql-autoscaler .

# 3. Run the complete end-to-end scaling test
./test.sh
