set -e

cmake -DCMAKE_PREFIX_PATH="$seastar_dir/build/dev;$seastar_dir/build/dev/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $my_app_dir
