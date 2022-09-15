var distribution_options_1 = {
          series: [{
          name: 'Distribution',
          type: 'column',
          data: feature_meta_1[op_index_1]['dist_plot'][1]
        }, {
          name: 'Distribution',
          type: 'area',
          data: feature_meta_1[op_index_1]['dist_plot'][1]
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
        labels: feature_meta_1[op_index_1]['dist_plot'][0],

        yaxis: {

          min: 0
        },
        };

        var bar_chart_distribution_1 = new ApexCharts(document.querySelector("#bar-chart-distribution-1"), distribution_options_1);
        bar_chart_distribution_1.render();
var distribution_options_2 = {
          series: [{
          name: 'Distribution',
          type: 'column',
          data: feature_meta_2[op_index_2]['dist_plot'][1]
        }, {
          name: 'Distribution',
          type: 'area',
          data: feature_meta_2[op_index_2]['dist_plot'][1]
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
        labels: feature_meta_2[op_index_2]['dist_plot'][0],

        yaxis: {

          min: 0
        },
        };

        var bar_chart_distribution_2 = new ApexCharts(document.querySelector("#bar-chart-distribution-2"), distribution_options_2);
        bar_chart_distribution_2.render();