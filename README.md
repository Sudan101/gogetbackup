# gogetbackup
 gogetbackup - discover exposed backup/archive files named after a target's domain
//
# Usage:
#   gogetbackup -u https://0xsudan.com
#   gogetbackup -l live_hosts.txt -c 40 -o found.txt
#   gogetbackup -u https://www.0xsudan.com -e ".zip,.tar,.sql.gz" -p "backup,db,site"
#--------------------------------------------------------------------------------------#
// For each target it derives candidate base names from the hostname
// (e.g. "0xsudan.com", "0xsudan", "www.oxsudan.com" -> "0xsudan") and combines them
// with a list of archive/backup extensions and optional filename prefixes,
// then requests <baseURL>/<candidate> and reports the ones that exist.
//
// Before scanning a host, it "calibrates" by requesting a random,
// definitely-nonexistent path so it can tell a real 200/OK apart from a
// custom error page or catch-all route that also returns 200 (a common
// source of false positives in this kind of brute force).
