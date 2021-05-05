
// Create the input graph
var g = new dagre.graphlib.Graph()
  .setGraph({nodesep: 100,
    ranksep: 50,
    rankdir: "LR",
    marginx: 20,
    marginy: 20})

  .setDefaultEdgeLabel(function() { return {curve: d3.curveBasis }; });
// Here we're setting nodeclass, which is used by our custom drawNodes function
// below.

for(var i=0;i<nodes.length;i++){
    g.setNode(i,nodes[i])
}
g.nodes().forEach(function(v) {
  var node = g.node(v);
  // Round the corners of the nodes
  node.rx = node.ry = 5;
});
for(var j=0;j<edge.length;j++){
    var link=edge[j]
    g.setEdge(link[0],link[1])
}


// Create the renderer
var render = new dagreD3.render();

// Set up an SVG group so that we can translate the final graph.

var svg = d3.select("svg"),
    svgGroup = svg.append("g");
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
render(d3.select("svg g"), g);
svgGroup.selectAll('g.node')
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

  // Center the graph
var xCenterOffset = (svg.attr("width") - g.graph().width) / 2;
svgGroup.attr("transform", "translate(" + xCenterOffset + ", 60)");

svg.attr("height", g.graph().height + 120);



