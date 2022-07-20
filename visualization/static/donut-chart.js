var options_donut = {
          series: [44, 55],
          labels: ['Entities', 'Relations'],
          chart: {
          type: 'donut',
        },
        dataLabels:{
            enabled:false,
        },
        legend: {
              position: 'right',
              horizontalAlign: 'center',
              offsetY: 80,
            },
        title: {
          text: 'Loaded Documents',
          align: 'left'
        },
        responsive: [{
          breakpoint: 480,
          options: {
            chart: {
              width: 200
            },
            legend: {
              position: 'right',
              horizontalAlign: 'center',
              offsetY: 10,
              floating: true,
            }
          }
        }]
        };

        var donut_chart = new ApexCharts(document.querySelector("#donut-chart"), options_donut);
        donut_chart.render();


