// Count submissions per email
db.metrics.aggregate([
  { $group: { _id: "$publisher_email", submissions: { $sum: 1 } } },
  { $sort: { submissions: -1 } }
]);

// Same, but with the latest timestamp
db.metrics.aggregate([
  { $group: {
      _id: "$publisher_email",
      submissions: { $sum: 1 },
      latest: { $max: "$timestamp" }
  }},
  { $sort: { submissions: -1 } }
]);

// List of individual submissions per email.
db.metrics.find(
  { publisher_email: "user@example.com" },
  { _id: 1, publisher_email: 1, timestamp: 1 }
).sort({ timestamp: -1 });

// First and last submission timestamp for each email
db.metrics.aggregate([
  {
    $group: {
      _id: "$publisher_email",
      firstTimestamp: { $min: "$timestamp" },
      lastTimestamp: { $max: "$timestamp" },
      count: { $sum: 1 }
    }
  },
  { $sort: { _id: 1 } }
]);

// List unique sites per publisher_email
const email = "example@email.com";
db.metrics.aggregate([
  { $match: { publisher_email: email } },
  { $unwind: "$body" },
  { $project: { site: { $ifNull: ["$body.Site", "$body.SiteName"] } } },
  { $match: { site: { $nin: [null, ""] } } },
  { $group: { _id: "$site" } },
  { $sort: { _id: 1 } }
], { allowDiskUse: true }).toArray();

// One sample document per publisher_email:
db.metrics.aggregate([
  { $sort: { timestamp: -1 } },
  {
    $group: {
      _id: "$publisher_email",
      oneMetrics: { $first: "$$ROOT" }
    }
  },
  { $project: { _id: 0, publisher_email: "$_id", oneMetric: 1 } },
  { $sort: { publisher_email: 1 } }
]);

// Hard-coded list of emails: run one query per email.
const emails = [
  "list of emails 01",
  "list of emails 02"
];

for (const email of emails) {
  print(`\n=== ${email} ===`);
  db.metrics.aggregate([
    { $match: { publisher_email: email } },
    { $sort: { timestamp: -1 } },
    { $limit: 1 },
    {
      $project: {
        _id: 1,
        publisher_email: 1,
        timestamp: 1,
        body: {
          $cond: [
            { $isArray: "$body" },
            { $arrayElemAt: ["$body", 0] },
            "$body"
          ]
        }
      }
    }
  ]).forEach(doc => printjson(doc));
}
}
