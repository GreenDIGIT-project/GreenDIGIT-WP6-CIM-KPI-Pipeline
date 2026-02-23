-- post.lua
wrk.method = "POST"
wrk.body   = io.open("../_test_requests/01_raw.json", "r"):read("*a")
wrk.headers["Content-Type"] = "application/json"