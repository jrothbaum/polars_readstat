class SharedConfig:
    """Shared configuration for all dependencies"""
    
    @staticmethod
    def apply_options(conanfile):
        """Apply shared build options to any conanfile"""
        conanfile.options["*"].shared = False
        conanfile.options["*"].fPIC = True
        
        # Common boost settings
        if "boost" in str(conanfile.requires):
            conanfile.options["boost"].without_python = True
            
        # Common spdlog settings  
        if "spdlog" in str(conanfile.requires):
            conanfile.options["spdlog"].shared = False
            
        # Common fmt settings
        if "fmt" in str(conanfile.requires):
            conanfile.options["fmt"].shared = False
            
        # Common arrow settings
        if "arrow" in str(conanfile.requires):
            conanfile.options["arrow"].shared = False
            conanfile.options["arrow"].fPIC = True
            conanfile.options["arrow"].compute = False
            conanfile.options["arrow"].acero = False
            conanfile.options["arrow"].csv = False
            conanfile.options["arrow"].cuda = False
            conanfile.options["arrow"].dataset = False
            conanfile.options["arrow"].flight_rpc = False
            conanfile.options["arrow"].gandiva = False
            conanfile.options["arrow"].hdfs = False
            conanfile.options["arrow"].hive = False
            conanfile.options["arrow"].json = False
            conanfile.options["arrow"].orc = False
            conanfile.options["arrow"].parquet = False
            conanfile.options["arrow"].s3 = False
            conanfile.options["arrow"].skyhook = False
            conanfile.options["arrow"].substrait = False
            conanfile.options["arrow"].with_brotli = False
            conanfile.options["arrow"].with_bz2 = False
            conanfile.options["arrow"].with_lz4 = False
            conanfile.options["arrow"].with_snappy = False
            conanfile.options["arrow"].with_zlib = False
            conanfile.options["arrow"].with_zstd = False
            conanfile.options["arrow"].with_boost = False
            conanfile.options["arrow"].with_gcs = False
            conanfile.options["arrow"].with_grpc = False
            conanfile.options["arrow"].with_jemalloc = False
            conanfile.options["arrow"].with_mimalloc = False
            conanfile.options["arrow"].with_openssl = False
            conanfile.options["arrow"].with_opentelemetry = False
            conanfile.options["arrow"].with_protobuf = False
            conanfile.options["arrow"].with_re2 = False
            conanfile.options["arrow"].with_thrift = False
            conanfile.options["arrow"].with_utf8proc = False
            conanfile.options["arrow"].simd_level = "default"