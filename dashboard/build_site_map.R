library(whisker)
library(httr)
tpl <- '
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
 {{#links}}
   <url>
      <loc>{{{loc}}}</loc>
      <lastmod>{{{lastmod}}}</lastmod>
   </url>
 {{/links}}
</urlset>
'
links <- c("https://LTREB-reservoirs.github.io/vera4cast/catalog.html",
           "https://LTREB-reservoirs.github.io/vera4cast/instructions.html",
           "https://LTREB-reservoirs.github.io/vera4cast/daily.html",
           "https://LTREB-reservoirs.github.io/vera4cast/index.html")

links <- c(links, paste0("https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/vera4cast/",fs::dir_ls(path = 'stac', glob="*.json", recurse=TRUE)))


map_links <- function(l) {
  tmp <- GET(l)
  d <- tmp$headers[['last-modified']]

  list(loc=l,
       lastmod=format(as.Date(d,format="%a, %d %b %Y %H:%M:%S")))
}

links <- lapply(links, map_links)

sink(file = "dashboard/docs/sitemap.xml")
cat(whisker.render(tpl))
