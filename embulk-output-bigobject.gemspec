
Gem::Specification.new do |spec|
  spec.name          = "embulk-output-bigobject"
  spec.version       = "0.1.0"
  spec.authors       = ["randyviola"]
  spec.summary       = "Bigobject output plugin for Embulk"
  spec.description   = "Dumps records to Bigobject."
  spec.email         = ["randyh0329@gmail.com"]
  spec.licenses      = ["MIT"]
  # TODO set this: spec.homepage      = "https://github.com/randyh0329/embulk-output-bigobject"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  #spec.add_dependency 'YOUR_GEM_DEPENDENCY', ['~> YOUR_GEM_DEPENDENCY_VERSION']
  spec.add_development_dependency 'embulk', ['~> 0.7.10']
  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end