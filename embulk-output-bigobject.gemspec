
Gem::Specification.new do |spec|
  spec.name          = "embulk-output-bigobject"
  spec.version       = "0.3.4"
  spec.authors       = ["Cheng-Ching Huang"]
  spec.summary       = "Bigobject output plugin for Embulk"
  spec.description   = "Dumps records to Bigobject."
  spec.email         = ["cchuang.tw@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/bigobject-inc/embulk-output-bigobject"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  spec.add_dependency 'rest-client', ['>= 1.8.0']
  #spec.add_development_dependency 'rest-client', ['>= 1.8.0']
  spec.add_development_dependency 'embulk', ['>= 0.8.9']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.5.0']
end
