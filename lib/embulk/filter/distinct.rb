Embulk::JavaPlugin.register_filter(
  "distinct", "org.embulk.filter.distinct.DistinctFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
