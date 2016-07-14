require "bundler/gem_tasks"

task default: :build

task :test do
	sh "embulk run -l debug -L . config.yml"
end
