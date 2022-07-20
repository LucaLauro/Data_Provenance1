var options = {
          series: [{name:'Relations',
                    data: [{
                    x: Date.now(),
                    y: 0
                }],
            }],
          colors:['#E9A722'],
          chart: {
          height: 250,
          type: 'area',
          zoom: {
            enabled: false
          },
          toolbar:{
              tools:{
                    download:false
                }
            },
          animations: {
            enabled: true,
            easing: 'linear',
            dynamicAnimation: {
              speed: 500
            }
           }
        },
        dataLabels: {
          enabled: false
        },
        stroke: {
          width: 5,
          curve: 'smooth'
        },
        title: {
          text: 'Loaded Relations',
          align: 'left'
        },
        grid: {
          row: {
            colors: ['#f3f3f3', 'transparent'], // takes an array which will be repeated on columns
            opacity: 0.5
          },
        },
        xaxis: {
        labels: {
                format: 'HH:mm:ss',
              },
          type: 'datetime'
        },
        yaxis:{
            labels:{
                show:false,
            }
        },
        tooltip:{
            x:{
            format: 'HH:mm:ss',
            },
        },
        };

        var chart2 = new ApexCharts(document.querySelector("#chart_relations"), options);

        chart2.render();

(window.setInterval(function(){
            $.ajax({
                url:"/data_relations",
                type:"GET",
                dataType:"html",
                success: function(data1){

                    var as1 = JSON.parse(data1);
                    chart2.clearAnnotations()
                   for(let i=0; i<as1[1].length; i++){
                    chart2.addXaxisAnnotation({
                        x: as1[1][i].time,
                        strokeDashArray: 0,
                        borderColor: '#775DD0',
                        label: {
                          borderColor: '#775DD0',
                          style: {
                                color: '#fff',
                                background: '#775DD0',
                              },
                            text: as1[1][i].function_name
                        },});
                    if(Date.now()-as1[1][i].time<1000){
                        chart2.appendData([{data: [{x: as1[1][i].time, y: as1[1][i].relations}]}])
                        }
                    };
                    chart2.appendData(as1[0]);
                },
            });
        }, 1000));
