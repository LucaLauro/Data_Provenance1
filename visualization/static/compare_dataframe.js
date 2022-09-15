
function update_text() {
      document.getElementById("index_num").innerText=df_meta[op_index]['N_Index'];
      document.getElementById("col_num").innerText=df_meta[op_index]['N_Column'];
            document.getElementById("code_activity").innerText='Line: '+df_meta[op_index]['CodeLine']+' Code: '+df_meta[op_index]['Code'];

      const list = document.getElementById("feature_names");
      var features=''
      for (const feature_index in df_meta[op_index]['Columns']) {
          console.log(feature_index)
          features+='<h5 class="text-center">'+df_meta[op_index]['Columns'][feature_index]+'</h5>';
      }
      list.innerHTML = features;
      var percentage_categories=[]
      var percentage_values=[]
      for (const index in df_meta[op_index]['NaNPercent']) {
          percentage_values.push(df_meta[op_index]['NaNPercent'][index][0])
          percentage_categories.push(df_meta[op_index]['NaNPercent'][index][1])
      }
      chart_bar_percentage.updateOptions({
         xaxis: {
            categories: percentage_categories
         },
         series: [{
            data: percentage_values
         }],
      });
      var corrmatrix=[]
      for (const index in df_meta[op_index]['CorrMatrix'][1]) {
          corrmatrix.push({name:df_meta[op_index]['CorrMatrix'][1][index],data: df_meta[op_index]['CorrMatrix'][0][index]})
      };
      console.log(corrmatrix);
      chart_corr_matrix.updateOptions({
         xaxis: {
            categories: df_meta[op_index]['CorrMatrix'][1]
         },
         series: corrmatrix.reverse(),
      });


     multiradialChart.updateOptions({
         labels: df_meta[op_index]['Dtype_percent'][1],
         series: df_meta[op_index]['Dtype_percent'][0],
      });

}



// Create the input graph
var g = new dagre.graphlib.Graph()
  .setGraph({nodesep: 100,
    ranksep: 50,
    rankdir: "RL",
    marginx: 20,
    marginy: 20})

  .setDefaultEdgeLabel(function() { return {curve: d3.curveBasis }; });
// Here we're setting nodeclass, which is used by our custom drawNodes function
// below.

for (var key in nodes) {
  g.setNode(key,nodes[key])
}
g.nodes().forEach(function(v) {
  var node = g.node(v);
  // Round the corners of the nodes
  node.rx = node.ry = 5;
});
for(var j=0;j<edge.length;j++){
    var link=edge[j]
    g.setEdge(link[0],link[1],{label:link[2]})
}

// Create the renderer
var render = new dagreD3.render();
// Set up an SVG group so that we can translate the final graph.

var svg = d3.select("#svg-canvas"),
    svgGroup = svg.append("g");
console.log(svg)
console.log(svgGroup)
    /*
    zoom = d3.zoom().on("zoom", function() {
    svgGroup.attr("transform", d3.event.transform);
  });

svg.call(zoom);
*/
var tooltip = d3.select("body")
              .append("div")
              .attr('id', 'tooltip_template')
              .style("position", "absolute")
              .style("background-color", "rgba(255,255,255,0.9)")
              .style("border", "solid")
              .style("border-width", "1px")
              .style("border-radius", "5px")
              .style("padding", "5px")
              .style("z-index", "10")
              .style("visibility", "hidden")
              .style("font-family", "Helvetica Neue")
              .text("...");




// Run the renderer. This is what draws the final graph.
render(d3.select("#svg-canvas g"), g);
svgGroup.selectAll('g.node.activity')
.attr("data-tooltip", function(v) {
      return g.node(v).description
  })
  .on("mouseover", function(){return tooltip.style("visibility", "visible");})
  .on("mousemove", function(){
  tooltip.html( this.dataset.tooltip)
      .style("top", (event.pageY-10)+"px")
      .style("left",(event.pageX+10)+"px");
})
  .on("mouseout", function(){return tooltip.style("visibility", "hidden");});
svg.attr("width", g.graph().width + 120);


svgGroup.selectAll('g.node.dfmeta')
.attr("data-tooltip", function(v) {
      return g.node(v).label
  })
  .on("mouseover", function(){return tooltip.style("visibility", "visible");})
  .on("mousemove", function(){
  tooltip.html( this.dataset.tooltip)
      .style("top", (event.pageY-10)+"px")
      .style("left",(event.pageX+10)+"px");
})
  .on("mouseout", function(){return tooltip.style("visibility", "hidden");})
   ;



svg.attr("width", g.graph().width + 120);
  // Center the graph
var xCenterOffset = (svg.attr("width") - g.graph().width) / 2;
svgGroup.attr("transform", "translate(" + xCenterOffset + ", 60)");

svg.attr("height", g.graph().height + 120);



