/*=========================================================================================
    File Name: dashboard-ecommerce.js
    Description: dashboard ecommerce page content with Apexchart Examples
    ----------------------------------------------------------------------------------------
    Item Name: Frest HTML Admin Template
    Version: 1.0
    Author: PIXINVENT
    Author URL: http://www.themeforest.net/user/pixinvent
==========================================================================================*/


var percentage_categories_1=[]
      var percentage_values_1=[]
      for (const index in df_meta[op_index_1]['NaNPercent']) {
          percentage_values_1.push(df_meta[op_index_1]['NaNPercent'][index][0])
          percentage_categories_1.push(df_meta[op_index_1]['NaNPercent'][index][1])
      }
var colors_1 = [];
while (colors_1.length < 100) {
    do {
        var color_1 = Math.floor((Math.random()*1000000)+1);
    } while (colors_1.indexOf(color_1) >= 0);
    colors_1.push("#" + ("000000" + color_1.toString(16)).slice(-6));
}
var bar_chart_percentage_options_1 = {
          series: [{
          data: percentage_values_1
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
        colors: colors_1,
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
          categories: percentage_categories_1,
          labels: {
            style: {
              colors: colors_1,
              fontSize: '12px'
            }
          }
        }
        };

        var chart_bar_percentage_1 = new ApexCharts(document.querySelector("#bar-chart-percentage-1"), bar_chart_percentage_options_1);
        chart_bar_percentage_1.render();



var percentage_categories_2=[]
      var percentage_values_2=[]
      for (const index in df_meta[op_index_2]['NaNPercent']) {
          percentage_values_2.push(df_meta[op_index_2]['NaNPercent'][index][0])
          percentage_categories_2.push(df_meta[op_index_2]['NaNPercent'][index][1])
      }
var colors_2= [];
while (colors_2.length < 100) {
    do {
        var color_2 = Math.floor((Math.random()*1000000)+1);
    } while (colors_2.indexOf(color_2) >= 0);
    colors_2.push("#" + ("000000" + color_2.toString(16)).slice(-6));
}
var bar_chart_percentage_options_2 = {
          series: [{
          data: percentage_values_2
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
        colors: colors_2,
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
          categories: percentage_categories_2,
          labels: {
            style: {
              colors: colors_2,
              fontSize: '12px'
            }
          }
        }
        };

        var chart_bar_percentage_2 = new ApexCharts(document.querySelector("#bar-chart-percentage-2"), bar_chart_percentage_options_2);
        chart_bar_percentage_2.render();



        var corrmatrix_1=[]
      for (const index in df_meta[op_index_1]['CorrMatrix'][1]) {
          corrmatrix_1.push({name:df_meta[op_index_1]['CorrMatrix'][1][index],data: df_meta[op_index_1]['CorrMatrix'][0][index]})
      };
  var correlation_matrix_option_1 = {

          series: corrmatrix_1.reverse(),
  xaxis: {
          categories: df_meta[op_index_1]['CorrMatrix'][1]
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

        var chart_corr_matrix_1 = new ApexCharts(document.querySelector("#correlation-matrix-chart-1"), correlation_matrix_option_1);
        chart_corr_matrix_1.render();
  var corrmatrix_2=[]
      for (const index in df_meta[op_index_2]['CorrMatrix'][1]) {
          corrmatrix_2.push({name:df_meta[op_index_2]['CorrMatrix'][1][index],data: df_meta[op_index_2]['CorrMatrix'][0][index]})
      };
  var correlation_matrix_option_2 = {

          series: corrmatrix_2.reverse(),
  xaxis: {
          categories: df_meta[op_index_2]['CorrMatrix'][1]
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

        var chart_corr_matrix_2 = new ApexCharts(document.querySelector("#correlation-matrix-chart-2"), correlation_matrix_option_2);
        chart_corr_matrix_2.render();
var multiRadialOptions_1 = {
        labels: df_meta[op_index_1]['Dtype_percent'][1],
          series: df_meta[op_index_1]['Dtype_percent'][0],
          chart: {
            height: 200,

          type: 'donut',
        },legend: {
    show: false
  },


        };

  var multiradialChart_1 = new ApexCharts(
    document.querySelector("#multi-radial-chart-1"),
    multiRadialOptions_1
  );
  multiradialChart_1.render();
  var multiRadialOptions_2 = {
        labels: df_meta[op_index_2]['Dtype_percent'][1],
          series: df_meta[op_index_2]['Dtype_percent'][0],
          chart: {
            height: 200,

          type: 'donut',
        },legend: {
    show: false
  },


        };

  var multiradialChart_2 = new ApexCharts(
    document.querySelector("#multi-radial-chart-2"),
    multiRadialOptions_2
  );
  multiradialChart_2.render();

