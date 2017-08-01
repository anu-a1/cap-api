var express = require('express');
var sql = require("mssql");
var router = express.Router();

var dbConfig = {
    driver: 'msnodesqlv8',
    server: "devlawsondb",
    database: "LawsonDW",
    options: {
        trustedConnection: true
    }
};

//Congiuration file

/* GET users listing. */
router.get('/', function(req, res, next) {
    // Comment out this line:
    res.send('Connected!!');
});

/**********************************Prepare Allocations****************************************************/


router.get('/PrepareAllocations', function (req, res, next) {
    try{
        var conn = new sql.Connection(dbConfig);

        var requestSQL = new sql.Request(conn);
        conn.connect(function (err) {
            if (err) {
                console.log(err);
                return;
            }
            requestSQL.execute("testproc", function (err) {
                //requestSQL.query("SELECT TOP 10 * FROM CAP.ALLOC_PERCENTAGES", function (err, recordset) {
                if (err) {
                    res.json({ Success: false,error:err });
                }
                else {
                    res.json({Success:true});
                }
                conn.close();
            });
        });}
    catch(e)
    {
        console.log(e);
    }

});

router.get('/GetPreparedAllocations', function(req, res, next) {

    var conn = new sql.Connection(dbConfig);

    conn.connect(function (err) {
        if (err) {
            console.log(err);
            return;
        }

        var requestSQL = new sql.Request(conn);
        requestSQL.input('ALLOCATION_MDL', sql.VarChar(32), 'PCP');
        requestSQL.execute("[CAP].[REVIEW_DATA]", function (err, recordset) {
            if (err) {
                res.json(err);
            }
            else {
                // res.json({ Success: true, data: recordset });
                res.send(recordset);
            }
            conn.close();
        });
    });

});

/**********************************End of Prepare Allocations****************************************************/

/**********************************Calculate Allocations****************************************************/
router.get('/CalculateAllocations', function(req, res, next) {

    var conn = new sql.Connection(dbConfig);

    var requestSQL = new sql.Request(conn);
    conn.connect(function (err) {
        if (err) {
            console.log(err);
            return;
        }
        // request.input('@FISCAL_YEAR',sql.Int);
        // request.input('@PERIOD',sql.Int);
        // request.input('@ALLOCATION_MDL',sql.VarChar(32));
        requestSQL.execute("[CAP].[FIGGEN_ALLOCATIONPROCESS]", function (err, recordset) {
            if (err) {
                res.json(err);
            }
            else {
                res.json(recordset);
            }
            conn.close();
        });
    });

});

router.get('/GetCalculatedAllocations', function(req, res, next) {

    var conn = new sql.Connection(dbConfig);

    var requestSQL = new sql.Request(conn);
    conn.connect(function (err) {
        if (err) {
            console.log(err);
            return;
        }
        request.input('@ALLOCATION_MDL',sql.VarChar(32),'PCP');
        requestSQL.execute("[CAP].[FIGGEN_ALLOCATIONPROCESS]", function (err, recordset) {
            if (err) {
                res.json(err);
            }
            else {
                res.json(recordset);
            }
            conn.close();
        });
    });

});

/**********************************End of Calculate Allocations****************************************************/
/**********************************Post Allocations****************************************************/
router.get('/PostAllocations', function(req, res, next) {

    var conn = new sql.Connection(dbConfig);

    var requestSQL = new sql.Request(conn);
    conn.connect(function (err) {
        if (err) {
            console.log(err);
            return;
        }
        requestSQL.query("SELECT TOP 10 * FROM CAP.ALLOC_PERCENTAGES", function (err, recordset) {
            if (err) {
                res.json(err);
            }
            else {
                res.json(recordset);
            }
            conn.close();
        });
    });

});

router.get('/GetPostedAllocations', function(req, res, next) {

    var conn = new sql.Connection(dbConfig);

    var requestSQL = new sql.Request(conn);
    conn.connect(function (err) {
        if (err) {
            console.log(err);
            return;
        }
        requestSQL.query("SELECT TOP 10 * FROM CAP.ALLOC_PERCENTAGES", function (err, recordset) {
            if (err) {
                res.json(err);
            }
            else {
                res.json(recordset);
            }
            conn.close();
        });
    });

});

/**********************************End of Post Allocations****************************************************/
module.exports = router;