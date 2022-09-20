from django.contrib.auth.models import User
from django.db import models


class Trade(models.Model):
    company = models.CharField(max_length=36, unique=True)
    price = models.FloatField()
    owner = models.ForeignKey(User, null=True, on_delete=models.SET_NULL)
