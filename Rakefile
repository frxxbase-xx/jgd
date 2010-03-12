task :default => [:error_check]

task :error_check do
  sh "compilejs --js jgd.js --warning_level VERBOSE --externs jgd.externs.js >/dev/null"
end
