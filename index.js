const express = require("express");
const app = express();
const Shopify = require("@shopify/shopify-api").Shopify;
require("dotenv").config();
const SQLite3 = require("sqlite3").verbose();
const AWS = require("aws-sdk");
var moment = require("moment");
const http = require("http");

const db = new SQLite3.Database("database.sqlite");
const { SHOP, STORE_ACCESS_TOKEN, SES_ACCESS_KEY, SES_SECRET, SES_REGION } =
  process.env;
const SES_CONFIG = {
  accessKeyId: SES_ACCESS_KEY,
  secretAccessKey: SES_SECRET,
  region: SES_REGION,
};
const AWS_SES = new AWS.SES(SES_CONFIG);

const query = (command, method = "all") => {
  return new Promise((resolve, reject) => {
    db[method](command, (error, result) => {
      if (error) {
        reject(error);
      } else {
        resolve(result);
      }
    });
  });
};

const createTable = async () => {
  await query(
    "CREATE TABLE IF NOT EXISTS abandon_carts (date text, checkout_date text, email text, checkout_token text, checkout_url text, line_items json, sent_cnt integer)",
    "run"
  );
  //interval in hour:1, 6, 10, 24
  //max_cnt: number of time to sent 1
  await query(
    "CREATE TABLE IF NOT EXISTS rules (interval integer, max_cnt integer)",
    "run"
  );
  var checkRule = await query(`SELECT * FROM rules WHERE rowid = 1`);
  if (checkRule.length > 0) {
  } else {
    // i.e sent 1times with 1hours interval
    await query(`INSERT INTO rules VALUES (1,1)`, "run");
  }
};

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const updateRules = async (interval, max_cnt) => {
  await query(
    `UPDATE rules SET interval="${interval}",max_cnt="${max_cnt}", sent_cnt=${cnt} WHERE rowid =  1`,
    "run"
  );
};
const updateCheckouts = async (
  date,
  checkout_date,
  checkout_token,
  checkout_url,
  cnt,
  email,
  is_sending
) => {
  if (is_sending == 1) {
    cnt++;
  }
  await query(
    `UPDATE abandon_carts SET date="${date}",checkout_date="${checkout_date}", checkout_token="${checkout_token}", checkout_url="${checkout_url}", sent_cnt=${cnt} WHERE email = "${email}"`,
    "run"
  );
};

const insertCheckouts = async (
  checkout_date,
  email,
  checkout_token,
  checkout_url,
  cnt,
  line_items
) => {
  const date = moment().format("YYYY-MM-DD HH:mm");
  const q = `SELECT email FROM abandon_carts WHERE email = "${email}"`;
  const ckemail = await query(q);
  if (ckemail?.length == 0) {
    query(
      `INSERT INTO abandon_carts VALUES ("${date}","${checkout_date}", "${email}", "${checkout_token}", "${checkout_url}", "${line_items}",0)`,
      "run"
    );
  } else {
    const old_date = moment(ckemail[0].checkout_date);
    const new_date = moment(checkout_date);
    const diff = new_date.diff(old_date)
    if ( diff > 0) {
      updateCheckouts(
        date,
        checkout_date,
        checkout_token,
        checkout_url,
        cnt,
        email,
        0
      );
    }
  }
};

let sendEmail = (recipientEmail, tempData) => {
  let params = {
    Source: "raysamtob@gmail.com",
    Template: "AbandonCartEmailTemplate",
    Destination: {
      ToAddresses: ["engsamtob@yahoo.com"],
    },
    TemplateData: tempData,
  };
  return AWS_SES.sendTemplatedEmail(params).promise();
};

const sendNotifications = async () => {
  var _rules = await query(`SELECT * FROM rules WHERE rowid = 1`);
  const _cnt = _rules[0].max_cnt;
  const _interval = _rules[0].interval;
  const existingCarts = await query(
    `SELECT rowid as id, date,checkout_date, email, checkout_url, checkout_token, sent_cnt FROM abandon_carts  WHERE sent_cnt < "${_cnt}"`
  );
  for (let i = 0; i < existingCarts.length; i++) {
    let existingCart = existingCarts[i];
    const email = existingCart.email;
    const last_sent_timestamp = moment(existingCart.date);
    const datenow = moment();
    var duration = moment.duration(datenow.diff(last_sent_timestamp));
    var sent_interval = duration.asHours();
    if (sent_interval >= _interval) {
      const date = datenow.format("YYYY-MM-DD HH:mm");
      const cnt = existingCart.sent_cnt;
      const checkout_url = existingCart.checkout_url;
      const checkout_token = existingCart.checkout_token;
      const checkout_date = existingCart.checkout_date;
      const checkout_unsubscribe = checkout_url.replace(
        "recover",
        "unsubscribe"
      );
      const tempData = `{"email": "${email}", "checkout_unsubscribe": "${checkout_unsubscribe}"}`;
      sendEmail(email, tempData);
      updateCheckouts(
        date,
        checkout_date,
        checkout_token,
        checkout_url,
        cnt,
        email,
        1
      );
    }
  }
};

db.serialize(async () => {
  createTable();
});

app.get("/set/rule", async (request, http_response) => {
  const interval = request.query.interval;
  const max_cnt = request.query.max_cnt;
  updateRules(interval, max_cnt);
  http_response.json({
    status:
      "Rule update to interval:  " + interval + " for max count: " + max_cnt,
  });
});
app.get("/abandon/notify", async (request, http_response) => {
  sendNotifications();
  http_response.json({ status: "ok" });
});
app.post;

app.get("/abandon/checkouts", async (request, http_response) => {
  const client = new Shopify.Clients.Rest(SHOP, STORE_ACCESS_TOKEN);
  var startdate = moment();
  startdate = startdate.subtract(1, "day").format("YYYY-MM-DD[T]HH:mm:ss");
  const checkouts = await client.get({
    path: "checkouts",
    // query: { created_at_max: startdate },
    query: { limit: "250" },
  });
  for (let i = 0; i < checkouts.body.checkouts.length; i++) {
    const checkout = checkouts.body.checkouts[i];
    const checkout_token = checkout.token;
    const checkout_email = checkout.email;
    const checkout_date = checkout.updated_at;
    let checkout_url = checkout.abandoned_checkout_url;
    const products = checkout.line_items;
    const lineItems = products;
    insertCheckouts(
      checkout_date,
      checkout_email,
      checkout_token,
      checkout_url,
      0,
      lineItems
    );
    await wait(20);
  }
  http_response.json({ status: "ok" });
});
const httpServer = http.createServer(app);
httpServer.listen(3000, () =>
  console.log("Abandon checkout notifier app is listening on port 3000.")
);
