var distribution_options = {
          series: [{
          name: 'Distribution',
          type: 'column',
          data: feature_meta[op_index]['dist_plot'][1]
        }, {
          name: 'Distribution',
          type: 'area',
          data: feature_meta[op_index]['dist_plot'][1]
        }],
          chart: {
          height: 350,
          type: 'line',
          stacked: false,
        },
        stroke: {
          width: [0, 2, 5],
          curve: 'smooth'
        },
        plotOptions: {
          bar: {
            columnWidth: '50%'
          }
        },

        fill: {
          opacity: [0.85, 0.25, 1],
          gradient: {
            inverseColors: false,
            shade: 'light',
            type: "vertical",
            opacityFrom: 0.85,
            opacityTo: 0.55,
            stops: [0, 100, 100, 100]
          }
        },
        labels: feature_meta[op_index]['dist_plot'][0],

        yaxis: {

          min: 0
        },
        };

        var bar_chart_distribution = new ApexCharts(document.querySelector("#bar-chart-distribution"), distribution_options);
        bar_chart_distribution.render();
