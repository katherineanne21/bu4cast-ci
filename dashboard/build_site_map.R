library(whisker)
library(httr)

config <- yaml::read_yaml('challenge_configuration.yaml')

tpl <- '<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
 {{#links}}
   <url>
      <loc>{{{loc}}}</loc>
      <lastmod>{{{lastmod}}}</lastmod>
   </url>
 {{/links}}
</urlset>
'
links <- c(paste0(config$challenge_url, "/catalog.html"),
           paste0(config$challenge_url, "/targets.html"),
           paste0(config$challenge_url, "/instructions.html"),
           paste0(config$challenge_url, "/performance.html"),
           paste0(config$challenge_url, "/index.html"))

links <- c(links, paste0("https://radiantearth.github.io/stac-browser/#/external/raw.githubusercontent.com/", config$github_repo, "/main/",fs::dir_ls(path = 'catalog', glob="*.json", recurse=TRUE)))

map_links <- function(l) {
  tmp <- GET(l)
  d <- tmp$headers[['last-modified']]

  list(loc=l,
       lastmod=format(as.Date(d,format="%a, %d %b %Y %H:%M:%S")))
}

links <- lapply(links, map_links)

cat(whisker.render(tpl), file = "dashboard/docs/sitemap.xml")
