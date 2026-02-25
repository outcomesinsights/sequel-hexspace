require "bundler/gem_tasks"

### Test 

desc "Run tests"
task :test do
  sh "#{FileUtils::RUBY} test/all.rb"
end

task :default => :test

begin
  require 'rubocop/rake_task'

  RuboCop::RakeTask.new(:lint) do |task|
    task.options = ['--display-cop-names']
  end

  RuboCop::RakeTask.new(:format) do |task|
    task.options = ['--auto-correct-all']
  end

  desc 'Run RuboCop with safe autocorrect'
  task :lint_fix do
    system('bundle exec rubocop --autocorrect')
  end
rescue LoadError
  # RuboCop not available
end
