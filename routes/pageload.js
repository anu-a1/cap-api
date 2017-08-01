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


/* GET users listing. */
router.get('/', function(req, res, next) {
    // Comment out this line:
    res.send('Connected!!' , req.headers['ntlm-proxy-username']);
});


router.get('/pageload', function (req, res, next) {
    try{
        var conn = new sql.Connection(dbConfig);

        var requestSQL = new sql.Request(conn);
        conn.connect(function (err) {
            if (err) {
                console.log(err);
                return;
            }
            requestSQL.execute("CAP.GET_LASTRUN_INFO", function (err,recordset) {

                if (err) {
                    res.json({ Success: false,error:err });
                }
                else {
                    res.setHeader("Content-Type", "text/html");
                    res.send(recordset);
                }
                conn.close();
            });
        });}
    catch(e)
    {
        console.log(e);
    }

});

router.get('/homepageinfo', function (req, res, next) {
    try{
        var conn = new sql.Connection(dbConfig);

        var requestSQL = new sql.Request(conn);
        conn.connect(function (err) {
            if (err) {
                console.log(err);
                return;
            }
            requestSQL.execute("CAP.GET_HOMEPAGE_INFO", function (err,recordset) {

                if (err) {
                    res.json({ Success: false,error:err });
                }
                else {
                    res.setHeader("Content-Type", "text/html");
                    res.send(recordset);
                }
                conn.close();
            });
        });}
    catch(e)
    {
        console.log(e);
    }

});


module.exports = router;