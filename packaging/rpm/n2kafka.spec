Name:    n2kafka
Version: %{__version}
Release: %{__release}%{?dist}

License: GNU AGPLv3
URL: https://gitlab.redborder.lan/dfernandez.ext/n2kafka/repository/archive.tar.gz?ref=redborder
Source0: %{name}-%{version}.tar.gz

BuildRequires: gcc librd-devel json-c-devel librdkafka-devel libev-devel libmicrohttpd-devel jansson-devel >= 2.7 yajl-devel >= 2.1.0 libcurl-devel >= 7.48

Summary: Network messages to json/kafka gateway
Group:   Development/Libraries/C and C++
Requires: librd0 json-c librdkafka1 libev libmicrohttpd jansson >= 2.7 yajl >= 2.1.0 libcurl >= 7.48
%description
%{summary}

%prep
%setup -qn %{name}-%{version}

%build
export CFLAGS=" -fcommon"
./configure --prefix=/usr
make

%install
DESTDIR=%{buildroot} make install
mkdir -p %{buildroot}/usr/share/n2kafka
install -D -m 644 n2kafka.service %{buildroot}/usr/lib/systemd/system/n2kafka.service
install -D -m 644 configs_example/n2kafka_config.json.default %{buildroot}/usr/share/n2kafka

%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(755,root,root)
/usr/bin/n2kafka
%defattr(644,root,root)
/usr/lib/systemd/system/n2kafka.service
/usr/share/n2kafka/n2kafka_config.json.default

%changelog
* Wed May 11 2016 Juan J. Prieto <jjprieto@redborder.com> - 1.0-1
- first spec version


