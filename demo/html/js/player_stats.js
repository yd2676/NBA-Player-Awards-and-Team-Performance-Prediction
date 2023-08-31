var dataSet=[];
var columnsData=[];
d3.csv("../csv/2023 Player Stats.csv",function(error,csvdata){  
    console.log(csvdata.columns);  
    
    //output csv name
    for(var i=0;i<csvdata.columns.length;i++){
        columnsData.push({"title":csvdata.columns[i]});
    }

    //output csv content
    for( var i=0; i<csvdata.length; i++ ){
        var nowData=[];
        for(var j=0;j<csvdata.columns.length;j++){
            if (j<=3||j==8){
                var columns=csvdata[i][csvdata.columns[j]];
            }  
            else{
                var columns=(csvdata[i][csvdata.columns[j]]*100).toFixed(1) + '%';
            }
            nowData.push(columns);
        }
        dataSet.push(nowData);
    }

    $('#demo').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="example"></table>' );
 
    $('#example').dataTable( {
        "data": dataSet,
        "columns": columnsData
    } );

});  