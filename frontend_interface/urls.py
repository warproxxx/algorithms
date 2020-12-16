from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('add_parms/', views.addParms, name='addParms'),
    path('daddy/', views.daddy_interface, name='daddy_interface'),
    path('reverse_status/', views.reverse_status, name='reverse_status'),
    path('delete/', views.delete, name='delete'),
    path('login/', views.adminLogin, name='login'),
    path('logout/', views.adminLogout, name='logout'),
]