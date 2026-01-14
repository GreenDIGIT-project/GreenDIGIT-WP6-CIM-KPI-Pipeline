const fs = require("fs");
const path = require("path");
const env = (typeof process !== "undefined" && process.env) ? process.env : {};
const emails = JSON.parse(env.EMAILS_JSON || "[]");
if (!emails.length) {
  print("No emails provided.");
  quit(0);
}

const dbName = env.DB_NAME || "metricsdb";
const collName = env.COLL_NAME || "metrics";
const analysisDir = env.ANALYSIS_DIR || "analysis";
const allowDiskUse = (env.ALLOW_DISK_USE || "").toLowerCase() === "true";

const coll = db.getSiblingDB(dbName).getCollection(collName);

if (!fs.existsSync(analysisDir)) {
  fs.mkdirSync(analysisDir, { recursive: true });
}

emails.forEach((email) => {
  const total = coll.countDocuments({ publisher_email: email });
  print("Processed " + email + ": total=" + total);
  const perMonth = coll.aggregate(
    [
      { $match: { publisher_email: email } },
      { $addFields: { ts: { $toDate: "$timestamp" } } },
      {
        $group: {
          _id: { year: { $year: "$ts" }, month: { $month: "$ts" } },
          submissions: { $sum: 1 }
        }
      },
      { $sort: { "_id.year": 1, "_id.month": 1 } },
      { $project: { _id: 0, year: "$_id.year", month: "$_id.month", submissions: 1 } }
    ],
    { allowDiskUse }
  ).toArray();
  const latestDoc = coll.find({ publisher_email: email }).sort({ timestamp: -1 }).limit(1).toArray()[0] || null;
  if (latestDoc && latestDoc._id && typeof latestDoc._id.toString === "function") {
    latestDoc._id = latestDoc._id.toString();
  }
  if (latestDoc && latestDoc.timestamp instanceof Date) {
    latestDoc.timestamp = latestDoc.timestamp.toISOString();
  }

  const out = { email, total, perMonth, latest: latestDoc };
  const slug = email.replace(/[^a-z0-9]+/gi, "_") || "metrics";
  const outPath = path.join(analysisDir, `${slug}.json`);
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  print("  -> saved " + outPath);
});
