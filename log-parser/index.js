var program = require('commander'); 
var NginxParser = require('nginxparser');
var firebase = require('firebase-admin');
var jwt_decode = require('jwt-decode');
var moment = require('moment')
var config = require('./config.json')


program
    .version('0.1.0')
    .option('-w , --watch [log-path] [environment]', 'Process and watch log')
    .option('-r, --run [log-path] [environment]', 'Process log')
    .parse(process.argv);

const getAll = async (collection, query) => {
    return new Promise(function (resolve, reject) {
        const { size, order, order_field, where } = query || {}
        let _size = size ? parseInt(size, 10) : 100
        _size = _size > 100 ? 500 : _size
        let ref = firedb.collection(collection)
        ref = order_field ? ref.orderBy(order_field, order && order === "asc" ? "asc" : 'desc') : ref
        // ref = ref.orderBy('created', 'desc')
        if (where) {
            if (typeof where === 'string') {
                // try {
                //     const filters = JSON.parse(where)
                // } catch (error) {
                //     console.log("unable to parse json")
                // }
            } else {
                for (let ix = 0; ix < where.length; ix++) {
                    const filter = where[ix];
                    ref = ref.where(filter[0], filter[1], filter[2])
                }
            }
        }
        ref = ref.limit(_size)
        ref.get()
            .then(snapshot => {
                const results = snapshot.docs.map(doc => ({ ...doc.data(), id: doc.id }))
                if (results.length > 0) {
                    resolve(results)
                } else {
                    resolve(results)
                }
            })
            .catch((err) => {
                console.log(err)
                reject([])
            })
    })
}

const get = async (collection, id) => {
    return new Promise(function (resolve, reject) {
        let ref = firedb.collection(collection).doc(id)
        ref.get()
            .then(doc => {
                if (doc.exists) {
                    const record = ({ ...doc.data(), id: doc.id })
                    resolve(record)
                } else {
                    resolve({ error: `${id} does not exist` })
                }
            })
            .catch((err) => {
                console.log(err)
                reject({})
            })
    })
}

const create = async (collection, data) => {
    return new Promise(function (resolve, reject) {
        const ref = firedb.collection(collection)
        const created = new Date().getTime()
        ref.add({ ...data, created, modified: created })
            .then(docRef => {
                resolve({ ...data, id: docRef.id })
            })
            .catch(err => reject(err))
    })
}

const batchCreate = async (collection, data) => {
    return new Promise(function (resolve, reject) {
        var batch = firedb.batch();
        const created = new Date().getTime()
        for (let index = 0; index < data.length; index++) {
            const item = data[index];
            const ref = firedb.collection(collection).doc()
            // console.log({ ...item, created, modified: created })
            batch.set(ref, { ...item, created, modified: created })
        }
        batch.commit()
            .then(() => {
                resolve(true)
            })
            .catch(err => {
                console.log(err)
                reject(false)
            })
    })
}

/*
Update an existing item in a collection
*/

const update = async (collection, id, data) => {
    return new Promise(function (resolve, reject) {
        let ref = firedb.collection(collection).doc(id)
        const modified = new Date().getTime()
        ref.update({ ...data, modified })
            .then(docRef => {
                resolve({ ...data, modified, id })
            })
            .catch(err => reject(err))
    })
}

const processLogs = async (tail = false, paths, environment ) => {
    console.log(environment)
    var serviceAccount = environment === 'staging' ? require('./staging.json') : require('./production.json');
    firebase.initializeApp({
        credential: firebase.credential.cert(serviceAccount),
        databaseURL: "https://udux-next.firebaseio.com"
    });
    const firedb = firebase.firestore(firebase)
    firedb.settings({ timestampsInSnapshots: true })
    const path = paths === true ? config.path : paths
    var parser = new NginxParser('$remote_addr - $remote_user [$time_local] ' + '"$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"');
    try {
       await  parser.read(path, { tail }, function (row) {
            const valid_field = {
                remote_addr: true,
                http_user_agent: true,
                time_local: true,
                request: true,
                status: true,
                body_bytes_sent: true,
                ip_str: true,
                request: true
            }
            const body = row
            const flow = {}
            for (let each in valid_field) {
                flow[each] = body[each]
            }
            const token = flow["request"] && flow["request"].split(' ')[1] ? flow["request"].split(' ')[1].split('/')[3] : ''
            const accesskey = flow["request"] && flow["request"].split(' ')[1] ? flow["request"].split(' ')[1].split('/')[2] : ''
            try {
                const run = async () => {
                    try {
                        const parsed = token ? await jwt_decode(token) : {}
                        const _partner = accesskey ? accesskey : ''
                        const _track = token ? await get('tracks', parsed['track_id']) : {}
                        delete _track['metadata']
                        delete flow['request']
                        delete parsed['exp']
                        delete parsed['iat']
                        flow['stream_time'] = flow['time_local'] ? moment(flow['time_local'].split(':')[0], "DD/MMM/YYYY").toDate().getTime() : Date.now()
                        const body = { ...parsed, ...flow, ..._track }
                        body['name'] = "stream_track"
                        const finito = { ...body, ... { client_id: _partner } }
                        delete finito['source']
                        delete finito['file']
                        return finito;
                    }
                    catch (error) {
                        return error;
                    }
                }
                const updateStatistic = (body, i = 0) => {
                    i++
                    return new Promise(async (resolve, reject) => {
                        const result = await getAll('statistics', { where: [["track_id", "==", `${body.track_id}`], ["user_id", "==", `${body.user_id}`], ["stream_time", "==", body.stream_time], ["device", "==", `${body.device}`]] })
                        if (result.length > 0 === false) {
                            create('statistics', body).then(data => {
                                return resolve(data)
                            }).catch(err => {
                                return i < 4 ? updateStatistic(data, i) : fs.appendFile('error.log', `${JSON.stringify(data)},\n`, function (err) {
                                    if (err) {
                                        console.log(err);
                                    }
                                    else {
                                        console.log('Updated!');
                                    }
                                });
                            })
                        }
                        return "Exist"
                    })

                }

               return run().then((data) => {
                    data.album ? updateStatistic(data) : 'Skip'
                }).catch((error) => {
                    fs.appendFile('error.log', `${JSON.stringify(data)},\n`, function (err) {
                        if (err) {
                            console.log(err);
                        }
                        else {
                            console.log('Updated!');
                        }
                    });
                })

            }
            catch (error) {
                console.log(error);
            }

        }, function (err) {
            if (err) {
                //  console.log("Invalid body");
            }
            else {
                //  console.log('Done!')
            }

        });
    }
    catch (error) {
        // console.log("Unable to parse")
    }
}

if (program.args[0]==="staging") {
if (program.watch) {
    console.log("Started")
    return processLogs(true, program.watch, 'staging').then((data) => console.log(data)).catch((error => { console.log(error) }));
}
else if (program.run) {
    console.log("Started")
    return processLogs(false, program.run, 'staging').then((data) => console.log(data)).catch((error => { console.log(error) }));
}
else {
    console.log("Invalid command")
}
}
else if (program.args[0] === "production"){
    if (program.watch) {
        return processLogs(true, program.watch, 'production').then((data) => console.log(data)).catch((error => { console.log(error) }));
    }
    else if (program.run) {
        console.log("Started")
        return processLogs(false, program.run, 'production').then((data) => console.log(data)).catch((error => { console.log(error) }));
    }
    else {
        console.log("Invalid command")
    }
}
else{
    console.log("Invalid Environment. For help: node index.js --help")
}