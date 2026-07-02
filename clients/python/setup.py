"""Packaging customization: this distribution ships a prebuilt native sidecar
binary (in dynamodb_streams_consumer/_bin/), so the wheel must be
platform-specific rather than a pure-Python `any` wheel. Metadata lives in
pyproject.toml; this file only overrides the wheel tag."""

from setuptools import setup

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel

    class bdist_wheel(_bdist_wheel):
        def finalize_options(self):
            super().finalize_options()
            # Mark the wheel impure → platform tag (e.g. linux_x86_64) instead of `any`.
            self.root_is_pure = False

        def get_tag(self):
            # The binary has no Python ABI dependency, so keep it py3/none but
            # platform-specific: (py3, none, <platform>).
            _py, _abi, plat = super().get_tag()
            return "py3", "none", plat

    cmdclass = {"bdist_wheel": bdist_wheel}
except ImportError:  # wheel not available (e.g. sdist-only build)
    cmdclass = {}

setup(cmdclass=cmdclass)
