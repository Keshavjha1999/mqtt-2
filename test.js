const mqtt = require('mqtt');
const moment = require('moment');
const Pool = require('pg-pool');
const cron = require('node-cron');

var pool = new Pool({
    database: 'bus_log',
    user: 'postgres',
    password: 'obcd1234',
    port: 5432,
    max: 20,
    // host: '172.17.0.1' // set pool max size to 20
    // idleTimeoutMillis: 1000, // close idle clients after 1 second
    // connectionTimeoutMillis: 1000, // return an error after 1 second if connection could not be established
    // maxUses: 7500, // close (and replace) a connection after it has been used 7500 times (see below for discussion)
});

// var db = pool.connect();

let topic = '/dist/wbtc_public/single_busdata';
var opts = {
    username: 'distronix',
    password: 'D@1357902468',
}

const client = new mqtt.connect('mqtt://pis.distronix.in', opts);

client.on('connect', function(){
    console.log("Connected");
})

client.subscribe(topic, function(err){
    console.log("error");
})

client.on('message', function (topic, message){
    txt = message.toString();
    const obj = JSON.parse(txt);
    let timestamp = moment(obj["timestamp"]).format('YYYY-MM-DD HH:mm:ss');
    // console.log(timestamp);
    // console.log(obj);
    quer = `Insert into bus_data (latitude, longitude, speed, vehicleregno, routecode, timestamp) values ( `+obj["latitude"]+`, ` +
    obj["longitude"] + `, ` + obj["speed"] + `, '` + obj["vehicleRegNo"] + `', '` + obj["routeCode"] + `', '` + timestamp + `');`;
    pool.query(quer).then(res => {
        console.log("Data Ingesting");
    }).catch(e =>{
        console.log(e.message);
    });
})

nodeCron.schedule("00 22 * * * *", () => {
    quer = 'Delete from bus_data where EXTRACT(DAY from CURRENT_TIMESTAMP - timestamp) > 4';
    pool.query(quer).then(res => {
        console.log("Data Deleted");
    }).catch(e => {
        console.log(e.message);
    })

    quer = 'VACUUM bus_data';
    pool.query(quer).then(res => {
        console.log("Vacuum Done");
    }).ctach(e => {
        console.log(e.message);
    })
});
