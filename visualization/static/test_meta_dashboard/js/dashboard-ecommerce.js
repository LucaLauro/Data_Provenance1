/*=========================================================================================
    File Name: dashboard-ecommerce.js
    Description: dashboard ecommerce page content with Apexchart Examples
    ----------------------------------------------------------------------------------------
    Item Name: Frest HTML Admin Template
    Version: 1.0
    Author: PIXINVENT
    Author URL: http://www.themeforest.net/user/pixinvent
==========================================================================================*/


var percentage_categories=[]
      var percentage_values=[]
      for (const index in df_meta[op_index]['NaNPercent']) {
          percentage_values.push(df_meta[op_index]['NaNPercent'][index][0])
          percentage_categories.push(df_meta[op_index]['NaNPercent'][index][1])
      }
var colors = [];
while (colors.length < 100) {
    do {
        var color = Math.floor((Math.random()*1000000)+1);
    } while (colors.indexOf(color) >= 0);
    colors.push("#" + ("000000" + color.toString(16)).slice(-6));
}
var bar_chart_percentage_options = {
          series: [{
          data: percentage_values
        }],
          chart: {
          height: 270,
          type: 'bar',
          events: {
            click: function(chart, w, e) {
              // console.log(chart, w, e)
            }
          }
        },
        colors: colors,
        plotOptions: {
          bar: {
            columnWidth: '45%',
            distributed: true,
          }
        },
        dataLabels: {
          enabled: false
        },
        legend: {
          show: false
        },
        xaxis: {
          categories: percentage_categories,
          labels: {
            style: {
              colors: colors,
              fontSize: '12px'
            }
          }
        }
        };

        var chart_bar_percentage = new ApexCharts(document.querySelector("#bar-chart-percentage"), bar_chart_percentage_options);
        chart_bar_percentage.render();



        var corrmatrix=[]
      for (const index in df_meta[op_index]['CorrMatrix'][1]) {
          corrmatrix.push({name:df_meta[op_index]['CorrMatrix'][1][index],data: df_meta[op_index]['CorrMatrix'][0][index]})
      };
  var correlation_matrix_option = {

          series: corrmatrix.reverse(),
  xaxis: {
          categories: df_meta[op_index]['CorrMatrix'][1]
        },
          chart: {
          height: 270,
          type: 'heatmap',
        },
        dataLabels: {
          enabled: false
        },
        colors: ["#008FFB"],

        };

        var chart_corr_matrix = new ApexCharts(document.querySelector("#correlation-matrix-chart"), correlation_matrix_option);
        chart_corr_matrix.render();

var multiRadialOptions = {
        labels: df_meta[op_index]['Dtype_percent'][1],
          series: df_meta[op_index]['Dtype_percent'][0],
          chart: {
            height: 260,

          type: 'donut',
        },

        responsive: [{
          breakpoint: 400,
          options: {
            chart: {
              width: 150
            },
            legend: {
              position: 'bottom'
            }
          }
        }]
        };




  var multiradialChart = new ApexCharts(
    document.querySelector("#multi-radial-chart"),
    multiRadialOptions
  );
  multiradialChart.render();
