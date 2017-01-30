#!/usr/bin/env python

import sys, os, os.path, shlex, subprocess
from subprocess import Popen as execScript
from distutils.core import setup
from distutils.command.bdist_rpm import bdist_rpm as _bdist_rpm

pkg_name = 'info-dynamic-scheduler-slurm'
pkg_version = '1.0.4'
pkg_release = '1'

source_items = "config setup.py src"

class bdist_rpm(_bdist_rpm):

    def run(self):

        topdir = os.path.join(os.getcwd(), self.bdist_base, 'rpmbuild')
        builddir = os.path.join(topdir, 'BUILD')
        srcdir = os.path.join(topdir, 'SOURCES')
        specdir = os.path.join(topdir, 'SPECS')
        rpmdir = os.path.join(topdir, 'RPMS')
        srpmdir = os.path.join(topdir, 'SRPMS')
        
        cmdline = "mkdir -p %s %s %s %s %s" % (builddir, srcdir, specdir, rpmdir, srpmdir)
        execScript(shlex.split(cmdline)).communicate()
        
        cmdline = "tar -zcf %s %s" % (os.path.join(srcdir, pkg_name + '.tar.gz'), source_items)
        execScript(shlex.split(cmdline)).communicate()
        
        specOut = open(os.path.join(specdir, pkg_name + '.spec'),'w')
        cmdline = "sed -e 's|@PKGVERSION@|%s|g' -e 's|@PKGRELEASE@|%s|g' project/%s.spec.in" % (pkg_version, pkg_release, pkg_name)
        execScript(shlex.split(cmdline), stdout=specOut, stderr=sys.stderr).communicate()
        specOut.close()
        
        cmdline = "rpmbuild -ba --define '_topdir %s' %s.spec" % (topdir, os.path.join(specdir, pkg_name))
        execScript(shlex.split(cmdline)).communicate()


setup(
      name='info-dynamic-scheduler-slurm',
      version=pkg_version,
      description='Information provider for SLURM',
      long_description='''Information provider for SLURM''',
      license='Apache Software License',
      author_email='CREAM group <cream-support@lists.infn.it>',
      packages=['SLURMInfoUtils'],
      package_dir = {'': 'src'},
      data_files=[
                  ('usr/libexec', ["src/info-dynamic-slurm", "src/lrmsinfo-slurm"])
                 ],
      cmdclass={'bdist_rpm': bdist_rpm}
     )


