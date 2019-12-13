from django.db import models

# Create your models here.
class ProxyDomain(models.Model):
    id=models.IntegerField(primary_key=True)
    domain = models.CharField(max_length=50)

    class Meta:
        managed = True
        app_label="hangzhou"
        db_table = 'proxy_ip_domain'

class ProxySet(models.Model):
    id=models.IntegerField(primary_key=True)
    ip = models.CharField(max_length=50)
    domain = models.CharField(max_length=2000, blank=True, null=True)
    expireTime = models.DateTimeField()

    class Meta:
        managed = True
        app_label="hangzhou"
        db_table = 'proxy_ip_zhima'