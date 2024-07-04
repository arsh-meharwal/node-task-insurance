const express = require("express");
require("dotenv").config();
const multer = require("multer");
const { Worker } = require("node:worker_threads");
const path = require("path");
const mongoose = require("mongoose");
const { MongoClient } = require("mongodb");
const os = require("os");
const { exec } = require("child_process");
const pidusage = require("pidusage");

const app = express();
app.use(express.json());
const port = 3000;

// Set up multer for file uploads
const upload = multer({ dest: "uploads/" });

// MongoDB connection string
const mongoUri = process.env.MONGODB;

// Endpoint to upload file
app.post("/upload", upload.single("file"), (req, res) => {
  if (!req.file) {
    return res.status(400).send("No file uploaded.");
  }

  const worker = new Worker(path.resolve(__dirname, "uploadWorker.js"), {
    workerData: {
      filePath: req.file.path,
      fileName: req.file.originalname,
      mongoUri,
    },
  });

  worker.on("message", (message) => {
    if (message.status === "done") {
      res.send("File uploaded and data inserted into MongoDB.");
    } else if (message.status === "error") {
      res.status(500).send("An error occurred: " + message.error);
    }
  });

  worker.on("error", (error) => {
    res.status(500).send("Worker error: " + error.message);
  });
});

app.post("/search", async (req, res) => {
  const { username } = req.body;
  if (!username) {
    return res.status(400).send("Username is required.");
  }
  let response = {};

  try {
    // Find the user by username (firstName)
    const client = new MongoClient(mongoUri);
    await client.connect();
    const db = client.db("test");
    const user = await db
      .collection("Data")
      .find({ firstName: username })
      .toArray();
    if (!user) {
      return res.status(404).send("User not found.");
    }
    user.map((item) => {
      (response.policyNumber = item.policyNumber),
        (response.policyCompany = item.policyCompany),
        (response.policyCategory = item.policyCategory),
        (response.policyStartDate = item.policyStartDate),
        (response.policyEndDate = item.policyEndDate);
    });
    // Find policies by userId
    res.json(response);
  } catch (error) {
    res.status(500).send("An error occurred: " + error.message);
  }
});

app.post("/message", async (req, res) => {
  const { message } = req.body;
  let date = new Date();
  const options = { weekday: "long" };
  const dayName = new Intl.DateTimeFormat("en-US", options).format(date);
  const istTime = date.toLocaleString("en-US", { timeZone: "Asia/Kolkata" });
  const newMessage = { message: message, day: dayName, time: istTime };
  const client = new MongoClient(mongoUri);
  await client.connect();
  const db = client.db("test");
  try {
    await db.collection("message").insertOne(newMessage);
    res.status(201).json(newMessage);
  } catch (error) {
    res.status(500).send("An error occurred: " + error.message);
  }
});

const getCpuUsage = async () => {
  try {
    const stats = await pidusage(process.pid);
    return stats.cpu; // CPU usage percentage
  } catch (error) {
    console.error("Error getting CPU usage:", error);
    return 0;
  }
};

const restartServer = () => {
  console.log("Restarting server due to high CPU usage...");
  process.exit(1); // Exit the process, supervisor (e.g., PM2) should restart it
};
// Function to monitor CPU and restart server if needed
const monitorCpuUsage = async () => {
  const cpuUsage = await getCpuUsage();
  console.log(`CPU Usage: ${cpuUsage}%`);
  if (cpuUsage > 70) {
    restartServer();
  }
};

async function main() {
  await mongoose.connect(mongoUri);
  console.log("DB Connected");
}
main();

app.listen(3000, () => {
  console.log(`Server is running`);
  setInterval(monitorCpuUsage, 1000);
});
