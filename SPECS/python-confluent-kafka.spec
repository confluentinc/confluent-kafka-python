%global pypi_name confluent-kafka

%global upstream_librdkafka_version 1.7.0

Name:           python-%{pypi_name}
Version:        1.7.0
Release:        1%{?dist}
Summary:        Confluent's Apache Kafka client for Python

License:        ASL 2.0
URL:            https://github.com/confluentinc/confluent-kafka-python
Source0:        https://files.pythonhosted.org/packages/source/c/%{pypi_name}/%{pypi_name}-%{version}.tar.gz

%description
confluent-kafka-python is Confluent's Python client for Apache Kafka
and the Confluent Platform.

%package -n     python3-%{pypi_name}
Summary:        %{summary}
%{?python_provide:%python_provide python3-%{pypi_name}}
BuildRequires:  gcc
BuildRequires:  librdkafka-devel >= %{upstream_librdkafka_version}
BuildRequires:  python3-devel
BuildRequires:  python3dist(pytest)
BuildRequires:  python3-setuptools
BuildRequires:  python3-requests
BuildRequires:  /usr/bin/pathfix.py

Requires:       python3-requests
Requires:       librdkafka1 >= %{upstream_librdkafka_version}
%description -n python3-%{pypi_name}
confluent-kafka-python is Confluent's Python client for Apache Kafka
and the Confluent Platform.


%prep
%autosetup -n %{pypi_name}-%{version}
# Remove bundled egg-info
rm -rf %{pypi_name}.egg-info

%build
%py3_build

%install
%py3_install
# Fix ambiguous shebangs
pathfix.py -pni "%{__python3} %{py3_shbang_opts}" %{buildroot}%{python3_sitearch}/confluent_kafka
# Set executable bit for scripts that do not have it
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/avro/cached_schema_registry_client.py
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/avro/error.py
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/avro/load.py
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/avro/serializer/__init__.py
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/avro/serializer/message_serializer.py
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/kafkatest/verifiable_consumer.py
chmod +x %{buildroot}%{python3_sitearch}/confluent_kafka/kafkatest/verifiable_producer.py
# Remove license file installed in weird place
rm -f  %{buildroot}/%{_prefix}/LICENSE.txt

%check
export PYTHONPATH=$(echo ${PWD}/build/lib.%{_target_os}-${_build_arch}*/)
py.test-3 -v --noconftest --ignore=tests/test_SerializerError.py --ignore=tests/test_log.py --ignore=tests/avro --ignore=tests/integration --ignore=tests/schema_registry ./tests/

%files -n python3-%{pypi_name}
%license LICENSE.txt
%doc README.md
%{python3_sitearch}/confluent_kafka
%{python3_sitearch}/confluent_kafka-%{version}-py%{python3_version}.egg-info

%changelog
* Thu Dec 9  2021 Andrew Egelhofer <aegelhofer@confluent.io> - 1.7.0-1
- Rebased against upstream 1.7.0
- Removed python2 things
- Enabled tests

* Tue Jul 27 2021 Fedora Release Engineering <releng@fedoraproject.org> - 0.11.6-13
- Second attempt - Rebuilt for
  https://fedoraproject.org/wiki/Fedora_35_Mass_Rebuild

* Fri Jun 04 2021 Python Maint <python-maint@redhat.com> - 0.11.6-12
- Rebuilt for Python 3.10

* Wed Jan 27 2021 Fedora Release Engineering <releng@fedoraproject.org> - 0.11.6-11
- Rebuilt for https://fedoraproject.org/wiki/Fedora_34_Mass_Rebuild

* Wed Jul 29 2020 Fedora Release Engineering <releng@fedoraproject.org> - 0.11.6-10
- Rebuilt for https://fedoraproject.org/wiki/Fedora_33_Mass_Rebuild

* Tue May 26 2020 Miro Hrončok <mhroncok@redhat.com> - 0.11.6-9
- Rebuilt for Python 3.9

* Thu Jan 30 2020 Fedora Release Engineering <releng@fedoraproject.org> - 0.11.6-8
- Rebuilt for https://fedoraproject.org/wiki/Fedora_32_Mass_Rebuild

* Thu Oct 03 2019 Miro Hrončok <mhroncok@redhat.com> - 0.11.6-7
- Rebuilt for Python 3.8.0rc1 (#1748018)

* Mon Aug 19 2019 Miro Hrončok <mhroncok@redhat.com> - 0.11.6-6
- Rebuilt for Python 3.8

* Fri Jul 26 2019 Fedora Release Engineering <releng@fedoraproject.org> - 0.11.6-5
- Rebuilt for https://fedoraproject.org/wiki/Fedora_31_Mass_Rebuild

* Sat Feb 02 2019 Fedora Release Engineering <releng@fedoraproject.org> - 0.11.6-4
- Rebuilt for https://fedoraproject.org/wiki/Fedora_30_Mass_Rebuild

* Thu Jan 24 2019 Javier Peña <jpena@redhat.com> - 0.11.16-3
- Fix python2-futures requirement
- Fix python2-enum34 for CentOS 7

* Fri Jan 11 2019 Javier Peña <jpena@redhat.com> - 0.11.16-2
- Fixed ambiguous shebangs
- Corrected description lines to avoid rpmlint errors

* Wed Dec 12 2018 Javier Peña <jpena@redhat.com> - 0.11.6-1
- Initial package.

