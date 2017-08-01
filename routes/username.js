var express = require('express');
var sql = require("mssql");
var router = express.Router();
var username = require("username");


router.get('/', function (req, res, next) {
    try{
        var conn = new sql.Connection(dbConfig);

        var requestSQL = new sql.Request(conn);
        conn.connect(function (err) {
            if (err) {
                console.log(err);
                return;
            }

            var username = 'aachuri'

            requestSQL.input('USERNAME',sql.VarChar(50),username);
            requestSQL.execute("CAP.GET_USER_PERMISSIONS", function (err) {

                if (err) {
                    res.json({ Success: false,error:err });
                }
                else {
                    res.setHeader("Content-Type", "text/html");
                    res.json({Success:true, Username: username});
                }
                conn.close();
            });
        });}
    catch(e)
    {
        console.log(e);
    }

});


router.get('/username', function (req, res, next) {
    try{
            console.log(username().then(username =>
            {
                res.json(username);
                console.log(username);
            }));
       }
    catch(e)
    {
        console.log(e);
    }

});



module.exports = router;