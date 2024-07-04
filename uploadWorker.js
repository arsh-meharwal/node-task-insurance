const { workerData, parentPort } = require("worker_threads");
const fs = require("fs");
const path = require("path");
const { MongoClient, ObjectId } = require("mongodb");
const csvParser = require("csv-parser");

const { filePath, mongoUri, dbName } = workerData;

async function uploadData() {
  const client = new MongoClient(mongoUri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  try {
    await client.connect();
    const db = client.db(dbName);

    const agentData = [];
    const userData = [];
    const userAccountData = [];
    const policyCategoryData = [];
    const policyCarrierData = [];
    const policyInfoData = [];
    const consolidatedData = [];

    const parseCSV = () => {
      return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
          .pipe(csvParser())
          .on("data", (row) => {
            // Agent Collection
            if (row["agent"]) {
              agentData.push({ name: row["agent"] });
            }

            // User Collection
            const user = {
              firstName: row["firstname"],
              dob: row["dob"],
              address: row["address"],
              phoneNumber: row["phone"],
              state: row["state"],
              zipCode: row["zip"],
              email: row["email"],
              gender: row["gender"],
              userType: row["userType"],
            };

            userData.push(user);
            const data = {
              firstName: row["firstname"],
              dob: row["dob"],
              address: row["address"],
              phoneNumber: row["phone"],
              state: row["state"],
              zipCode: row["zip"],
              email: row["email"],
              gender: row["gender"],
              userType: row["userType"],
              agent: row["agent"],
              policyNumber: row["policy_number"],
              policyStartDate: row["policy_start_date"],
              policyEndDate: row["policy_end_date"],
              policyCompany: row["company_name"],
              policyCategory: row["category_name"],
            };
            consolidatedData.push(data);

            // User's Account Collection
            if (row["account_name"]) {
              userAccountData.push({ name: row["account_name"] });
            }

            // Policy Category Collection
            if (row["category_name"]) {
              policyCategoryData.push({ name: row["category_name"] });
            }

            // Policy Carrier Collection
            if (row["company_name"]) {
              policyCarrierData.push({ name: row["company_name"] });
            }

            // Policy Info Collection
            const policyInfo = {
              policyNumber: row["policy_number"],
              policyStartDate: row["policy_start_date"],
              policyEndDate: row["policy_end_date"],
            };
            policyInfoData.push(policyInfo);
          })
          .on("end", resolve)
          .on("error", reject);
      });
    };

    await parseCSV();

    // Insert data into collections if arrays are not empty
    if (agentData.length > 0)
      await db.collection("Agent").insertMany(agentData);
    if (consolidatedData.length > 0)
      await db.collection("Data").insertMany(consolidatedData);
    let insertedUsers, insertedPolicyCategories, insertedPolicyCarriers;

    if (userData.length > 0)
      insertedUsers = await db.collection("User").insertMany(userData);
    if (userAccountData.length > 0)
      await db.collection("UserAccount").insertMany(userAccountData);
    if (policyCategoryData.length > 0)
      insertedPolicyCategories = await db
        .collection("PolicyCategory")
        .insertMany(policyCategoryData);
    if (policyCarrierData.length > 0)
      insertedPolicyCarriers = await db
        .collection("PolicyCarrier")
        .insertMany(policyCarrierData);

    // Update policyInfoData with actual ObjectIds
    policyInfoData.forEach((policy, index) => {
      if (insertedPolicyCategories)
        policy.policyCategoryId = insertedPolicyCategories.insertedIds[index];
      if (insertedPolicyCarriers)
        policy.companyId = insertedPolicyCarriers.insertedIds[index];
      if (insertedUsers) policy.userId = insertedUsers.insertedIds[index];
    });

    if (policyInfoData.length > 0)
      await db.collection("PolicyInfo").insertMany(policyInfoData);

    await client.close();
    parentPort.postMessage({ status: "done" });
  } catch (error) {
    parentPort.postMessage({ status: "error", error: error.message });
  }
}

uploadData();
