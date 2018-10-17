/**
* second (0-59)
* minute (0-59)
* hour (0-23)
* date (1-31)
* month (0-11)
* year
* dayOfWeek (0-6) Starting with Sunday
* ex: rule.dayOfWeek = [0, new schedule.Range(4, 6)]
*
*/

var config = require('config')
var dbConfig = config.get('DBConfig')
var mysql = require('mysql2/promise')

var schedule = require('node-schedule')
var request = require('async-request')
const axios = require('axios')

// TITLE
console.log('BitPortal Wallet Info Sync Job Start - ' + new Date().toLocaleString())

/**
 * Main Task Function: BitPortal Wallet Info Sync Job
 */
var snapshotSyncTask = async () => {
  console.log('BitPortal Wallet Info Sync Job main function Start . . .')
  console.log('time: ', new Date().toLocaleString())

  // A-0: Check DB Connection √
  console.log(dbConfig)
  const dbConn = await mysql.createConnection(dbConfig)
  console.log('DB Connected √')

  // A-1: get Wallets List from MySQL
  console.log('retriving latest snapshot data . . .')
  var walletList = await getWalletList(dbConn) // NOTES: Get latest wallet list
  console.log('√ Wallet List Received')
  // B-1: get each wallet info from EOSpark API
  // C-1: Insert Wallet Info data -> bitportal_wallet_info
  var walletsInfoList = await updateWalletsInfo(walletList, dbConn)

  console.log('√ DB Insert completed!')
  dbConn.end()
  console.log('√ DB connection ended!')
}

/**
 * function A-1: Get latest wallet list
 */
var getWalletList = async (dbConn) => {
  console.log('start to retrieve latest wallet list . . .')
  try {
    const [result] = await dbConn.execute('select * from `bitportal_wallets`')
    let output = []
    result.forEach((ele) => {
      output.push(ele.walletId)
    })
    return output
  } catch (e) {
    console.log(e)
  }
}

  /**
 * function B-1: Get Wallet Info in Batch
 */
var updateWalletsInfo = async (walletsList, dbConn) => {
  console.log('start to retrieve wallet info . . .')
  let output = []
  try {
    for (let i = 0; i < walletsList.length; i++) {
      let balance = await getWalletBalance(walletsList[i])
      let wallet = { balance: balance, walletId: walletsList[i] }
      await walletInfoDBSave(wallet, dbConn)
      await sleep(0.45)
    }

    return output
  } catch (e) {
    console.log(e)
  }
}

/**
 * Function A-2: save snapshot to database
 */

var walletInfoDBSave = async (walletsInfo, dbConn) => {
    // start insert snapshot into MySQL
  console.log(walletsInfo)

//   let queries = ''
//   walletsInfoList.forEach(function (item) {
//     console.log(item)
//     queries += mysql.format('UPDATE `bitportal_wallets` SET balance = ? WHERE walletId in (?); ', item)
//   })
  // Insert into DB

  let query = 'UPDATE `bitportal_wallets` SET `balance` = ? , `updatedAt` = ? WHERE `walletId` = ?;'
  try {
    let output = await dbConn.query(query, [walletsInfo.balance, new Date().toLocaleString(), walletsInfo.walletId])
    //   console.log(output)
  } catch (e) {
    console.log(e)
  }
}

// Sleep x second
const sleep = async (time = 0) => {
  time *= 1000
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve()
    }, time)
  })
}

// Get Wallet Account Balance
const getWalletBalance = async (account) => {
  let url = 'https://api.eospark.com/api?module=account&action=get_account_balance&apikey=a9564ebc3289b7a14551baf8ad5ec60a&account=' + account
  let response, bp_data
  try {
    response = await axios.get(url)
    bp_data = response.data
  } catch (error) {
    console.error(error)
        // throw new Error(error.toString())
  }
  let acct_balance = bp_data.data.balance
  if (acct_balance === '') { acct_balance = 0 }
  console.log('account balance', account, acct_balance)
  return acct_balance
}

/**
 * SnapshotSync Job
 * rule: every 2 hours
 */

// var rule = new schedule.RecurrenceRule()
// rule = '0 0 */5 * * *' // production: every 5 hours
// // rule = '*/20 * * * * *' // development: 20s
// var snapshotSyncJob = schedule.scheduleJob(rule, snapshotSyncTask)

// DIRECT RUN - Since cronjob are using pm2 now, there is no need of runtime scheduler
snapshotSyncTask() // TEST_CODE
