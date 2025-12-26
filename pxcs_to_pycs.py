"""
PXCSToPYCS: 将 ROI 内像素坐标系(PXCS)点转换到物理坐标系(PYCS)。

约定（与题述一致）：
- PXCS 原点在 ROI 左上角；输入 (px, py) 为 ROI 内像素坐标（可为浮点，单位 pixel）。
- PYCS 原点在 ROI 中心；因此需要提供 ROI 中心在 PXCS 下的像素坐标 (pixel_center_x, pixel_center_y)。
- PXCS 的 Y 轴方向与 PYCS 的 Y 轴方向相反：计算 Yc 时需要取反。
- pixel_size 为“单像素物理值”（单位例如 mm/pixel、um/pixel 等），输出物理坐标与其一致。

转换步骤：
1) 由像素位置、像素中心、单像素值计算 Xc, Yc（Yc 取反）
2) 用标定参数 Mag 与 Kappa 计算 tempPycsX/tempPycsY（支持两种常见单参数径向畸变模型）
3) 用 CameraRz 旋转，得到最终 PYCS 下的 (X, Y)
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Iterable, List, Literal, Sequence, Tuple, Union, overload


KappaModel = Literal["division", "polynomial"]
AngleUnit = Literal["deg", "rad"]


@dataclass(frozen=True)
class PXCSToPYCSParams:
    """
    PXCS->PYCS 转换所需参数。

    - pixel_center_x/y: ROI 中心在 PXCS 的像素坐标（单位 pixel）
    - pixel_size: 单像素物理值（单位 物理单位/pixel）
    - mag: 放大倍率（常见做法：将像平面坐标除以 mag 得到物理坐标）
    - kappa: 单参数径向畸变系数
    - camera_rz: 绕 Z 轴旋转角（用于从“临时物理坐标系”旋转到最终图像物理坐标系）
    - camera_rz_unit: camera_rz 的单位，默认度
    - kappa_model:
        - "division":  x_u = x_d / (1 + kappa * r_d^2)
        - "polynomial": x_u = x_d * (1 + kappa * r_d^2)
      你题述未明确采用哪一种；工程里最常见的是 division 模型（单参数更稳定）。
    """

    pixel_center_x: float
    pixel_center_y: float
    pixel_size: float
    mag: float
    kappa: float
    camera_rz: float
    camera_rz_unit: AngleUnit = "deg"
    kappa_model: KappaModel = "division"


def _to_radians(angle: float, unit: AngleUnit) -> float:
    if unit == "rad":
        return angle
    if unit == "deg":
        return math.radians(angle)
    raise ValueError(f"Unsupported angle unit: {unit!r}")


def _step1_px_to_xc_yc(
    px: float,
    py: float,
    *,
    pixel_center_x: float,
    pixel_center_y: float,
    pixel_size: float,
) -> Tuple[float, float]:
    # 以 ROI 中心为原点，并把 pixel -> 物理单位
    xc = (px - pixel_center_x) * pixel_size
    # Y 轴方向相反：PXCS 向下为正，PYCS 向上为正
    yc = -(py - pixel_center_y) * pixel_size
    return xc, yc


def _step2_apply_mag_kappa(
    xc: float,
    yc: float,
    *,
    mag: float,
    kappa: float,
    kappa_model: KappaModel,
) -> Tuple[float, float]:
    if mag == 0:
        raise ValueError("mag must be non-zero")

    r2 = xc * xc + yc * yc

    if kappa_model == "division":
        # x_u = x_d / (1 + kappa r^2), 再除以 mag
        denom = (1.0 + kappa * r2) * mag
        if denom == 0:
            raise ValueError("division model denominator becomes zero; check kappa/r2/mag")
        return xc / denom, yc / denom

    if kappa_model == "polynomial":
        # x_u = x_d * (1 + kappa r^2), 再除以 mag
        scale = (1.0 + kappa * r2) / mag
        return xc * scale, yc * scale

    raise ValueError(f"Unsupported kappa_model: {kappa_model!r}")


def _step3_rotate_rz(
    x: float,
    y: float,
    *,
    camera_rz: float,
    camera_rz_unit: AngleUnit,
) -> Tuple[float, float]:
    theta = _to_radians(camera_rz, camera_rz_unit)
    c = math.cos(theta)
    s = math.sin(theta)
    # 绕 Z 轴旋转（右手系）
    out_x = x * c - y * s
    out_y = x * s + y * c
    return out_x, out_y


def PXCSToPYCS(
    px: float,
    py: float,
    *,
    pixel_center_x: float,
    pixel_center_y: float,
    pixel_size: float,
    mag: float,
    kappa: float,
    camera_rz: float,
    camera_rz_unit: AngleUnit = "deg",
    kappa_model: KappaModel = "division",
) -> Tuple[float, float]:
    """
    将单个点从 PXCS 转为 PYCS。

    参数含义见模块说明；返回 (X, Y) 为最终图像物理坐标系下的物理坐标。
    """

    xc, yc = _step1_px_to_xc_yc(
        px,
        py,
        pixel_center_x=pixel_center_x,
        pixel_center_y=pixel_center_y,
        pixel_size=pixel_size,
    )
    temp_x, temp_y = _step2_apply_mag_kappa(
        xc,
        yc,
        mag=mag,
        kappa=kappa,
        kappa_model=kappa_model,
    )
    return _step3_rotate_rz(
        temp_x,
        temp_y,
        camera_rz=camera_rz,
        camera_rz_unit=camera_rz_unit,
    )


def pxcs_to_pycs(px: float, py: float, params: PXCSToPYCSParams) -> Tuple[float, float]:
    """PXCSToPYCS 的参数对象版本。"""
    return PXCSToPYCS(
        px,
        py,
        pixel_center_x=params.pixel_center_x,
        pixel_center_y=params.pixel_center_y,
        pixel_size=params.pixel_size,
        mag=params.mag,
        kappa=params.kappa,
        camera_rz=params.camera_rz,
        camera_rz_unit=params.camera_rz_unit,
        kappa_model=params.kappa_model,
    )


def pxcs_to_pycs_many(
    points: Sequence[Sequence[float]],
    params: PXCSToPYCSParams,
) -> List[Tuple[float, float]]:
    """批量转换：points = [(px, py), ...] -> [(X, Y), ...]"""
    out: List[Tuple[float, float]] = []
    for p in points:
        if len(p) != 2:
            raise ValueError(f"Each point must be (px, py); got: {p!r}")
        out.append(pxcs_to_pycs(float(p[0]), float(p[1]), params))
    return out


if __name__ == "__main__":
    # 简单示例（把 ROI 中心点映射到 (0,0)）
    params = PXCSToPYCSParams(
        pixel_center_x=320.0,
        pixel_center_y=240.0,
        pixel_size=0.005,  # 例如 0.005 mm/pixel
        mag=1.0,
        kappa=0.0,
        camera_rz=0.0,
        camera_rz_unit="deg",
        kappa_model="division",
    )
    print("center ->", pxcs_to_pycs(320.0, 240.0, params))
