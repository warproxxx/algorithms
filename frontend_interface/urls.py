from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('daddy/', views.daddy_interface, name='daddy_interface'),
    path('vol_trend/', views.vol_trend_interface, name='vol_trend'),
    path('altcoin/', views.altcoin_interface, name='altcoin_interface'),
    path('add_parms/', views.addParms, name='addParms'),
    path('clear_log/', views.clearLog, name='clearLog'),
    path('reverse_status/', views.reverse_status, name='reverse_status'),
    path('delete/', views.delete, name='delete'),
    path('login/', views.adminLogin, name='login'),
    path('logout/', views.adminLogout, name='logout'),
]