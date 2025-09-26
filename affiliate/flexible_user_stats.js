const { MongoClient } = require('mongodb');

// MongoDB connection configuration
const MONGODB_URI = 'mongodb://xxx:xxx@localhost:27019/poker_data?authSource=admin&directConnection=true';

// PBCI User IDs (from your existing queries)
const PBCI_USER_IDS = [
  4815318, 5904209, 4826404, 4795548, 4794933, 4821545, 4818286,
  5997854, 4795769, 4797275, 4795447, 4800231, 4916635, 4827307,
  4798815, 4795847, 4807177, 4807833, 5078287, 4820784, 4794539,
  5998805, 4883380, 4866909, 4838441, 4815523, 4815339, 4818693,
  4817189, 4816997, 4817340, 4798145, 4805362, 4818022, 4794844,
  4807636, 4799794, 4796970, 4815857, 4806619, 4804826, 4823223,
  4816169, 4799478, 5911717, 4800477, 4904763, 4799274, 4818109,
  5998279, 5896453, 4932934, 4941180, 4820307, 4801304, 4807955,
  4801747, 4895351, 4877851, 4808125, 5732419, 4815526, 4807266,
  4797088, 5999024, 4797327, 4796305, 4800017, 4818076, 4795014,
  4796940
];

/**
 * Generate collection names for the given date range
 * @param {Date} startDate - Start date
 * @param {Date} endDate - End date
 * @returns {string[]} Array of collection names
 */
function getCollectionNames(startDate, endDate) {
  const collections = [];
  const current = new Date(startDate);
  
  while (current <= endDate) {
    const year = current.getFullYear();
    const month = String(current.getMonth() + 1).padStart(2, '0');
    collections.push(`game_${year}${month}`);
    
    // Move to next month
    current.setMonth(current.getMonth() + 1);
    current.setDate(1);
  }
  
  return collections;
}

/**
 * Create optimized aggregation pipeline for user statistics
 * @param {string[]} userIds - Array of user IDs
 * @param {Date} startDate - Start date
 * @param {Date} endDate - End date
 * @returns {Object[]} MongoDB aggregation pipeline
 */
function createUserStatsPipeline(userIds, startDate, endDate) {
  return [
    {
      $match: {
        gt: "RING",
        "users.uid": { $in: userIds },
        et: {
          $gte: startDate,
          $lt: endDate
        }
      }
    },
    {
      $unwind: {
        path: "$users",
        preserveNullAndEmptyArrays: false
      }
    },
    {
      $match: {
        "users.uid": { $in: userIds }
      }
    },
    {
      $addFields: {
        Bet_Amount: {
          $reduce: {
            input: {
              $filter: {
                input: "$gd",
                as: "entry",
                cond: { $eq: ["$$entry.pid", "$users.uid"] }
              }
            },
            initialValue: 0,
            in: {
              $add: [
                "$$value",
                {
                  $cond: {
                    if: { $eq: ["$$this.an", "uncalledAmt"] },
                    then: { $multiply: ["$$this.amt", -1] },
                    else: "$$this.amt"
                  }
                }
              ]
            }
          }
        },
        Win: {
          $reduce: {
            input: {
              $filter: {
                input: "$winners",
                as: "entry",
                cond: { $eq: ["$$entry.uid", "$users.uid"] }
              }
            },
            initialValue: 0,
            in: { $add: ["$$value", "$$this.amt"] }
          }
        },
        WinM: {
          $reduce: {
            input: {
              $filter: {
                input: "$mrwinners",
                as: "entry",
                cond: { $eq: ["$$entry.uid", "$users.uid"] }
              }
            },
            initialValue: 0,
            in: { $add: ["$$value", "$$this.amt"] }
          }
        },
        BpHandOneWin: {
          $reduce: {
            input: {
              $filter: {
                input: "$winners",
                as: "entry",
                cond: { $eq: ["$$entry.uid", "$users.uid"] }
              }
            },
            initialValue: 0,
            in: { $add: ["$$value", "$$this.amt"] }
          }
        },
        BpHandTwoWin: {
          $let: {
            vars: {
              firstPot: { $arrayElemAt: ["$potwinamtplayerlist", 0] }
            },
            in: {
              $let: {
                vars: {
                  winnerlistSize: {
                    $size: { $ifNull: ["$$firstPot.winnerlist", []] }
                  }
                },
                in: {
                  $let: {
                    vars: {
                      gramtAdjustment: {
                        $cond: {
                          if: { $gt: ["$$winnerlistSize", 0] },
                          then: {
                            $divide: [
                              "$gramt",
                              { $multiply: ["$$winnerlistSize", 2] }
                            ]
                          },
                          else: 0
                        }
                      }
                    },
                    in: {
                      $reduce: {
                        input: {
                          $filter: {
                            input: { $ifNull: ["$$firstPot.winnerlist", []] },
                            as: "entry",
                            cond: {
                              $eq: [
                                { $toString: "$$entry.uid" },
                                { $toString: "$users.uid" }
                              ]
                            }
                          }
                        },
                        initialValue: 0,
                        in: {
                          $add: [
                            "$$value",
                            { $subtract: ["$$this.amt", "$$gramtAdjustment"] }
                          ]
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },
    {
      $group: {
        _id: { gid: "$gid", uid: "$users.uid" },
        User_ID: { $first: "$users.uid" },
        User_Name: { $first: "$users.un" },
        Network_ID: { $first: "$users.nid" },
        Game_Type: { $first: "$gt" },
        Session_Id: { $first: "$users.id" },
        Start_Time: { $first: "$st" },
        End_Time: { $first: "$et" },
        Config_ID: { $first: "$cid" },
        Game_Format: { $first: "$gv" },
        Game_Variant: { $first: "$cn" },
        Hand_Id: { $first: "$gid" },
        Table_Id: { $first: "$tid" },
        SB_Amt: { $first: "$sb" },
        BB_Amt: { $first: "$bb" },
        Ante: { $first: "$ante" },
        SBAMT: { $first: "$users.sbamt" },
        BP: { $first: "$bp" },
        Small_Blind: { $first: { $ifNull: ["$users.sbamt", 0] } },
        Big_Blind: { $first: { $ifNull: ["$users.bbamt", 0] } },
        Post_Big_Blind: { $first: { $ifNull: ["$users.pbbamt", 0] } },
        Loss: { $first: "$users.pl" },
        Bet_Amount: { $sum: "$Bet_Amount" },
        WinM: { $sum: "$WinM" },
        Win: { $sum: "$Win" },
        BpHandOneWin: { $sum: "$BpHandOneWin" },
        BpHandTwoWin: { $sum: "$BpHandTwoWin" },
        Rake_Amount: { $sum: "$users.ramt" },
        Min_BB: { $min: "$minBB" },
        Max_BB: { $max: "$maxBB" },
        Game_state: { $first: "$gs" }
      }
    },
    {
      $addFields: {
        BP_Rake_Amount: { $multiply: ["$Rake_Amount", 2] },
        Total_Bet_Amt: {
          $cond: {
            if: { $eq: ["$BP", false] },
            then: {
              $add: [
                "$Small_Blind",
                "$Big_Blind",
                "$Post_Big_Blind",
                "$Bet_Amount"
              ]
            },
            else: {
              $add: ["$Ante", "$Bet_Amount"]
            }
          }
        },
        Total_Win_Amount: {
          $cond: {
            if: { $eq: ["$BP", false] },
            then: { $add: ["$WinM", "$Win"] },
            else: { $add: ["$BpHandOneWin", "$BpHandTwoWin"] }
          }
        },
        Total_Rake: {
          $cond: {
            if: { $eq: ["$BP", false] },
            then: "$Rake_Amount",
            else: {
              $cond: {
                if: { $and: [{ $eq: ["$BP", true] }, { $eq: ["$Game_state", "Show"] }] },
                then: "$BP_Rake_Amount",
                else: "$Rake_Amount"
              }
            }
          }
        }
      }
    },
    {
      $sort: { Hand_Id: 1, User_ID: 1 }
    },
    {
      $group: {
        _id: "$User_ID",
        User_Name: { $first: "$User_Name" },
        Network_ID: { $first: "$Network_ID" },
        Total_Bet_Amount: { $sum: "$Total_Bet_Amt" },
        Total_Win_Amount: { $sum: "$Total_Win_Amount" },
        Total_Rake: { $sum: "$Total_Rake" },
        Total_Hands: { $sum: 1 },
        Start_Date: { $min: "$Start_Time" },
        End_Date: { $max: "$End_Time" }
      }
    },
    {
      $sort: { Total_Rake: -1 }
    }
  ];
}

/**
 * Fetch user statistics for the given parameters
 * @param {string[]} userIds - Array of user IDs
 * @param {Date} startDate - Start date
 * @param {Date} endDate - End date
 * @returns {Promise<Object[]>} Array of user statistics
 */
async function fetchUserStats(userIds, startDate, endDate) {
  const client = new MongoClient(MONGODB_URI);
  
  try {
    await client.connect();
    console.log('Connected to MongoDB successfully');
    
    const db = client.db('poker_data');
    const collections = getCollectionNames(startDate, endDate);
    
    console.log(`Processing collections: ${collections.join(', ')}`);
    
    const pipeline = createUserStatsPipeline(userIds, startDate, endDate);
    
    // Use the first collection as the base and union with others
    let aggregationPipeline = [...pipeline];
    
    for (let i = 1; i < collections.length; i++) {
      aggregationPipeline.push({
        $unionWith: {
          coll: collections[i],
          pipeline: pipeline
        }
      });
    }
    
    // Final grouping to combine results from all collections
    aggregationPipeline.push({
      $group: {
        _id: "$_id",
        User_Name: { $first: "$User_Name" },
        Network_ID: { $first: "$Network_ID" },
        Total_Bet_Amount: { $sum: "$Total_Bet_Amount" },
        Total_Win_Amount: { $sum: "$Total_Win_Amount" },
        Total_Rake: { $sum: "$Total_Rake" },
        Total_Hands: { $sum: "$Total_Hands" },
        Start_Date: { $min: "$Start_Date" },
        End_Date: { $max: "$End_Date" }
      }
    });
    
    aggregationPipeline.push({
      $sort: { Total_Rake: -1 }
    });
    
    const result = await db.collection(collections[0]).aggregate(aggregationPipeline).toArray();
    
    return result;
    
  } catch (error) {
    console.error('Error fetching user stats:', error);
    throw error;
  } finally {
    await client.close();
  }
}

/**
 * Convert Decimal128 to number
 * @param {any} value - Value to convert
 * @returns {number} Converted number
 */
function convertDecimal128(value) {
  if (typeof value === 'object' && value && value.toString) {
    return parseFloat(value.toString());
  }
  return value || 0;
}

/**
 * Display results in a formatted table
 * @param {Object[]} results - Array of user statistics
 * @param {string} title - Title for the results
 */
function displayResults(results, title) {
  console.log(`\n=== ${title} ===`);
  console.log('User ID | User Name | Network | Total Bet | Total Win | Total Rake | Hands | Start Date | End Date');
  console.log('--------|-----------|---------|-----------|-----------|------------|-------|------------|----------');
  
  let totalBet = 0;
  let totalWin = 0;
  let totalRake = 0;
  let totalHands = 0;
  
  results.forEach(user => {
    const userId = user._id;
    const userName = user.User_Name || 'N/A';
    const networkId = user.Network_ID || 'N/A';
    
    const betAmount = convertDecimal128(user.Total_Bet_Amount);
    const winAmount = convertDecimal128(user.Total_Win_Amount);
    const rakeAmount = convertDecimal128(user.Total_Rake);
    const hands = user.Total_Hands || 0;
    const startDate = user.Start_Date ? new Date(user.Start_Date).toISOString().split('T')[0] : 'N/A';
    const endDate = user.End_Date ? new Date(user.End_Date).toISOString().split('T')[0] : 'N/A';
    
    console.log(`${userId} | ${userName.padEnd(9)} | ${networkId.toString().padEnd(7)} | ${betAmount.toFixed(2).padStart(9)} | ${winAmount.toFixed(2).padStart(9)} | ${rakeAmount.toFixed(2).padStart(10)} | ${hands.toString().padStart(5)} | ${startDate} | ${endDate}`);
    
    totalBet += betAmount;
    totalWin += winAmount;
    totalRake += rakeAmount;
    totalHands += hands;
  });
  
  console.log('--------|-----------|---------|-----------|-----------|------------|-------|------------|----------');
  console.log(`TOTALS  |           |         | ${totalBet.toFixed(2).padStart(9)} | ${totalWin.toFixed(2).padStart(9)} | ${totalRake.toFixed(2).padStart(10)} | ${totalHands.toString().padStart(5)} |            |          `);
  
  console.log(`\nSummary:`);
  console.log(`- Total Users: ${results.length}`);
  console.log(`- Total Bet Amount: $${totalBet.toFixed(2)}`);
  console.log(`- Total Win Amount: $${totalWin.toFixed(2)}`);
  console.log(`- Total Rake: $${totalRake.toFixed(2)}`);
  console.log(`- Total Hands: ${totalHands}`);
  console.log(`- Net Result: $${(totalWin - totalBet).toFixed(2)}`);
  
  return { totalBet, totalWin, totalRake, totalHands };
}

/**
 * Export results to CSV
 * @param {Object[]} results - Array of user statistics
 * @param {string} filename - Output filename
 */
function exportToCSV(results, filename) {
  const csvContent = [
    'User_ID,User_Name,Network_ID,Total_Bet_Amount,Total_Win_Amount,Total_Rake,Total_Hands,Start_Date,End_Date',
    ...results.map(user => {
      const betAmount = convertDecimal128(user.Total_Bet_Amount);
      const winAmount = convertDecimal128(user.Total_Win_Amount);
      const rakeAmount = convertDecimal128(user.Total_Rake);
      
      return [
        user._id,
        user.User_Name || 'N/A',
        user.Network_ID || 'N/A',
        betAmount,
        winAmount,
        rakeAmount,
        user.Total_Hands || 0,
        user.Start_Date ? new Date(user.Start_Date).toISOString().split('T')[0] : 'N/A',
        user.End_Date ? new Date(user.End_Date).toISOString().split('T')[0] : 'N/A'
      ].join(',');
    })
  ].join('\n');
  
  const fs = require('fs');
  fs.writeFileSync(filename, csvContent);
  console.log(`\nResults exported to: ${filename}`);
}

/**
 * Main function to run the PBCI user statistics query
 * @param {string[]} userIds - Array of user IDs (defaults to PBCI_USER_IDS)
 * @param {Date} startDate - Start date
 * @param {Date} endDate - End date
 * @param {string} title - Title for the results
 */
async function main(userIds = PBCI_USER_IDS, startDate, endDate, title = 'PBCI User Statistics') {
  try {
    console.log('Fetching user statistics...');
    console.log(`Date range: ${startDate.toISOString()} to ${endDate.toISOString()}`);
    console.log(`Number of users: ${userIds.length}`);
    
    const results = await fetchUserStats(userIds, startDate, endDate);
    
    const totals = displayResults(results, title);
    
    // Export to CSV
    const filename = `user_stats_${new Date().toISOString().split('T')[0]}.csv`;
    exportToCSV(results, filename);
    
    return { results, totals };
    
  } catch (error) {
    console.error('Error in main function:', error);
    process.exit(1);
  }
}

// Command line interface
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.length < 2) {
    console.log('Usage: node flexible_user_stats.js <start_date> <end_date> [user_ids...]');
    console.log('Example: node flexible_user_stats.js 2025-06-01 2025-06-30');
    console.log('Example: node flexible_user_stats.js 2025-06-01 2025-06-30 4883380 4895351 4801304');
    process.exit(1);
  }
  
  const startDate = new Date(args[0] + 'T00:00:00.000Z');
  const endDate = new Date(args[1] + 'T23:59:59.999Z');
  
  let userIds = PBCI_USER_IDS;
  if (args.length > 2) {
    userIds = args.slice(2).map(id => parseInt(id));
  }
  
  const title = `User Statistics (${args[0]} to ${args[1]})`;
  
  main(userIds, startDate, endDate, title);
}

module.exports = {
  fetchUserStats,
  createUserStatsPipeline,
  getCollectionNames,
  displayResults,
  exportToCSV,
  convertDecimal128,
  PBCI_USER_IDS
};

