#!/bin/bash -l

set -e

CWDIR=$( cd $( dirname ${BASH_SOURCE[0]} ) && pwd )
source ${CWDIR}/common.bash


install_pxf() {
  local hdfsrepo=$1
  if [ -d pxf_tarball ]; then
    echo "======================================================================"
    echo "                            Install PXF"
    echo "======================================================================"
    pushd pxf_tarball > /dev/null
    unpack_tarball ./*.tar.gz
    for X in distributions/pxf-*.tar.gz; do
      tar -xvzf ${X}
    done
    mkdir -p ${hdfsrepo}/pxf/conf
    mv pxf-*/pxf-*.jar ${hdfsrepo}/pxf
    mv pxf-*/pxf.war ${hdfsrepo}/pxf
    mv pxf-*/conf/{pxf-public.classpath,pxf-profiles.xml} ${hdfsrepo}/pxf/conf
    popd > /dev/null
    pushd ${hdfsrepo}/pxf && for X in pxf-*-[0-9]*.jar; do \
      ln -s ${X} $(echo ${X} | sed -e 's/-[a-zA-Z0-9.]*.jar/.jar/'); \
    done
    popd > /dev/null
  fi
}

unpack_tarball() {
  local tarball=$1
  echo "Unpacking tarball: $(ls ${tarball})"
  tar xfp ${tarball} --strip-components=1
}

symlink_build_dir() {
  local target_base_dir=$1
  local cwd=$2
  if [ ! -d ${target_base_dir} ]; then
    ln -sfv ${cwd} ${target_base_dir}
  fi
}

setup_gpadmin_user() {
  # Don't error out if GPADMIN user already exists
  /usr/sbin/useradd gpadmin 2> /dev/null && \
      echo "gpadmin  ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/gpadmin || true
  groupadd supergroup 2> /dev/null || true
  usermod -G supergroup gpadmin
}

setup_environment() {
  export TERM=xterm-256color
  export TIMEFORMAT=$'\e[4;33mIt took %R seconds to complete this step\e[0m';

  setup_gpadmin_user
  symlink_build_dir "/home/build" $(pwd)
  echo "ssh setup already done"
  /etc/init.d/sshd start

  pushd singlecluster && if [ -f ./*.tar.gz ]; then \
    unpack_tarball ./*.tar.gz; \
  fi && popd
}

_main() {
  setup_environment
  install_pxf /home/build/singlecluster
}

_main "$@"
